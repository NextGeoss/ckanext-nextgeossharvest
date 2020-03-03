# -*- coding: utf-8 -*-
import re
import os

import json
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import uuid

from ftplib import FTP

from ckan.plugins.core import implements
from ckan.model import Session


from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.nextgeossharvest.lib.fsscat_base import FSSCATBase
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObjectExtra as HOExtra

from sqlalchemy import desc

from ckanext.nextgeossharvest.lib.fsscat_config import COLLECTION

from StringIO import StringIO


def parse_date_path(url):
    datepath = url.split('/')[-4:-1]
    return '-'.join(datepath)

def parse_filename(url):
    filename = url.split('/')[-1]
    return os.path.splitext(filename)[0]

class FSSCATHarvester(NextGEOSSHarvester, FSSCATBase):
    '''
    A Harvester for FSSCAT Products.
    '''
    implements(IHarvester)

    def __init__(self, *args, **kwargs):
        super(type(self), self).__init__(*args, **kwargs)
        self.overlap = timedelta(days=30)
        self.interval = timedelta(days=3 * 30)

    def info(self):
        return {
            'name': 'fsscat',
            'title': 'FSSCAT Harvester',
            'description': 'A Harvester for FSSCAT Products'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)
            product_types = [key for key in COLLECTION]
            if config_obj.get('harvester_type') not in product_types:
                error_template = "harvester type is required and must be one of {}"
                raise ValueError(error_template.format(product_types))  # noqa: E501
    
            if 'start_date' in config_obj:
                try:
                    start_date = config_obj['start_date']
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                except ValueError:
                    raise ValueError('start_date format must be yyyy-mm-dd')
            else:
                raise ValueError('start_date is required')

            if 'end_date' in config_obj:
                try:
                    end_date = config_obj['end_date']
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')
                except ValueError:
                    raise ValueError('end_date format must be yyyy-mm-dd')

                if not end_date > start_date:
                    raise ValueError('end_date must be after start_date')

            if type(config_obj.get('password', None)) != unicode:
                raise ValueError('password is required and must be a string')
            if type(config_obj.get('username', None)) != unicode:
                raise ValueError('username is requred and must be a string')
            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')
            if type(config_obj.get('update_all', False)) != bool:
                raise ValueError('update_all must be true or false')
            if type(config_obj.get('max_dataset', 100)) != int:
                raise ValueError('max_dataset must be an integer')
        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        self.log = logging.getLogger(__file__)
        self.log.debug('FSSCAT Harvester gather_stage for job: %r', harvest_job)

        self.source_config = self._get_config(harvest_job)
        ftp_user =  self.source_config.get('username')
        ftp_pwd =  self.source_config.get('password')
        source_type =  self.source_config.get('harvester_type')
        max_datasets =  self.source_config.get('max_datasets', 100)
        self.update_all =  self.source_config.get('update_all', False)

        last_product_date = (
            self._get_last_harvesting_date(harvest_job.source_id)
        )
        if last_product_date is not None:
            start_date = last_product_date
        else:
            start_date =  self.source_config.get('start_date')

        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date =  self.source_config.get('end_date', False)
        end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()

        ftp_source = create_ftp_source(source_type)

        products = ftp_source.get_products_path(start_date, end_date,
                                                ftp_user, ftp_pwd,
                                                max_datasets)

        ids = []
        for product in products:
            resources = ftp_source.get_resources_url(ftp_user, ftp_pwd,
                                                     product)
            manifest_str = "manifest.fssp.safe.xml"
            manifest_url = [resource for resource in resources if 
                            manifest_str in resource]
            if manifest_url:
                manifest_url = manifest_url[0]
                manifest_content = ftp_source.get_file_content(ftp_user,
                                                               ftp_pwd,
                                                               manifest_url)

            ids.append(self._gather_object(harvest_job, product, resources,
                                           manifest_content))

        return ids

    def fetch_stage(self, harvest_object):
        return True

    def _get_last_harvesting_date(self, source_id):
        objects = self._get_imported_harvest_objects_by_source(source_id)
        sorted_objects = objects.order_by(desc(HarvestObject.import_finished))
        last_object = sorted_objects.limit(1).first()
        if last_object is not None:
            restart_date = json.loads(last_object.content)['restart_date']
            return datetime.strptime(restart_date, '%Y-%m-%d')
        else:
            return None

    def _get_imported_harvest_objects_by_source(self, source_id):
        return Session.query(HarvestObject).filter(
            HarvestObject.harvest_source_id == source_id,
            HarvestObject.import_finished is not None)

    def _get_config(self, harvest_job):
        return json.loads(harvest_job.source.config)

    def _gather_object(self, job, product, resources, manifest_content):
        name = parse_filename(product).lower()
        restart_date = parse_date_path(product)

        status, package = self._was_harvested(name, self.update_all)

        extras = [HOExtra(key='status', value=status)]

        content = json.dumps({
            'name': name,
            'restart_date': restart_date,
            'manifest_content': manifest_content,
            'resources': resources
        }, default=str
        )

        obj = HarvestObject(job=job,
                            guid=uuid.uuid4(),
                            extras=extras,
                            content=content)
        obj.package = package
        obj.save()
        return obj.id


def create_ftp_source(source_type):
    return FtpSource(**COLLECTION[source_type]["source"])

class FtpSource(object):

    def __init__(self, domain, path):
        self.domain = domain
        self.path = path

    def get_products_path(self, start_date, end_date, user, pwd, max_datasets):
        ftp_urls = set()
        ftp = FTP(self._get_ftp_domain(), user, pwd)
        prod_type_exists = self._check_ftp_prod_type_path(ftp,
                                                          self._get_ftp_path())
        if not prod_type_exists:
            print("{} does not exists in the FTP.".format(self._get_ftp_path()))
            ftp.quit()
            return ftp_urls
        
        harvest_date = start_date
        products_count = 0

        while self._to_harvest(harvest_date, start_date, end_date,
                               products_count, max_datasets):

            ftp.cwd(self._get_ftp_path())
            date_path = self._date_path(harvest_date)
            path_list = date_path.strip("/").split("/")
            path_list_len = len(path_list)
            inverse_depth = self._check_ftp_date_path(ftp, path_list)
            
            if inverse_depth:
                fail_depth = path_list_len - inverse_depth
                harvest_date = self._get_new_harvest_date(harvest_date, fail_depth)
                if not harvest_date:
                    ftp.quit()
                    return ftp_urls

            else:

                new_product_count = products_count + len(ftp.nlst())
                _to_harvest = self._to_harvest(harvest_date, start_date,
                                               end_date, new_product_count,
                                               max_datasets)
                _to_harvest = _to_harvest or products_count == 0
                if _to_harvest:
                    ftp_urls |= set(self._ftp_path(date_path, product)
                                    for product in ftp.nlst())
                    
                    harvest_date += timedelta(days=1)
                    products_count = len(ftp_urls)
                else:
                    ftp.quit()
                    return ftp_urls
        ftp.quit()
        return ftp_urls

    def _get_new_harvest_date(self, harvest_date, fail_depth):
        if fail_depth == 0:
            # It did not find the year folder
            harvest_date += relativedelta(years=+1)
            new_harvest_date = datetime(harvest_date.year,1,1)
            return new_harvest_date
        elif fail_depth == 1:
            # It did not find the month folder
            harvest_date += relativedelta(months=+1)
            new_harvest_date = datetime(harvest_date.year,
                                        harvest_date.month,
                                        1)
            return new_harvest_date
        elif fail_depth == 2:
            # It did not find the day folder
            harvest_date += relativedelta(days=+1)
            new_harvest_date = datetime(harvest_date.year,
                                        harvest_date.month,
                                        harvest_date.day)
            return new_harvest_date
        else:
            # Hypothesis not contemplated
            print("FTP structure not recognized")
            return None 

    def _check_ftp_date_path(self, ftp, path_list):
        path = path_list.pop(0)
        if path in ftp.nlst():
            ftp.cwd("{}".format(path))
            if path_list:
                inverse_depth = self._check_ftp_date_path(ftp, path_list)
                return inverse_depth
            else:
                return 0
        else:
            return len(path_list) + 1

    def _check_ftp_prod_type_path(self, ftp, prod_type_path):
        path_list = prod_type_path.strip("/").split("/")
        for _path in path_list:
            if _path in ftp.nlst():
                ftp.cwd(_path)
            else:
                return False
        ftp.cwd("/")
        return True

    def _to_harvest(self, date, start, end, products_count, max_products):
        time_constraint = date >= start and date <= end
        product_constraint = (products_count <= max_products)
        return time_constraint and product_constraint

    def _ftp_path(self, directory, filename):
        return '{}/{}/{}'.format(self._get_ftp_path(),
                                 directory,
                                 filename)

    def _ftp_url(self, ftp_path, filename):
        domain_path = self._get_ftp_domain().strip("/")
        ftp_path = ftp_path.strip("/")
        return 'ftp://{}/{}/{}'.format(domain_path,
                                       ftp_path,
                                       filename)

    def _ftp_path_from_url(self, ftp_url):
        domain_path = self._get_ftp_domain().strip("/")
        str_to_remove = "ftp://{}".format(domain_path)
        return ftp_url.replace(str_to_remove, "")

    def _date_path(self, date):
        return date.strftime('%Y/%m/%d')

    def _get_ftp_domain(self):
        return self.domain

    def _get_ftp_path(self):
        return self.path

    def get_resources_url(self, user, pwd, dataset):
        resources = set()
        ftp = FTP(self._get_ftp_domain(), user, pwd)
        ftp.cwd(dataset)
        resources |=set(self._ftp_url(dataset, resource_url)
                        for resource_url in ftp.nlst())
        ftp.quit()
        return resources

    def get_file_content(self, user, pwd, manifest_url):
        ftp = FTP(self._get_ftp_domain(), user, pwd)
        ftp_path = self._ftp_path_from_url(manifest_url)
        r = StringIO()
        ftp.retrbinary('RETR {}'.format(ftp_path), r.write)
        ftp.quit()
        return r.getvalue()
