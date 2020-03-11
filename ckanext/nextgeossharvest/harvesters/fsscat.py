# -*- coding: utf-8 -*-
import json
import logging
import os
import re
import uuid
from datetime import datetime, timedelta
from ftplib import FTP
from StringIO import StringIO

from dateutil.relativedelta import relativedelta
from sqlalchemy import desc

from ckan.model import Session
from ckan.plugins.core import implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.nextgeossharvest.lib.fsscat_base import FSSCATBase
from ckanext.nextgeossharvest.lib.fsscat_config import COLLECTION
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

log = logging.getLogger(__name__)

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
            if type(config_obj.get('ftp_domain', None)) != unicode:
                raise ValueError('ftp_domain is required and must be a string')
            if type(config_obj.get('ftp_path', None)) != unicode:
                raise ValueError('ftp_path is required and must be a string')
            if type(config_obj.get('ftp_pass', None)) != unicode:
                raise ValueError('ftp_pass is required and must be a string')
            if type(config_obj.get('ftp_user', None)) != unicode:
                raise ValueError('ftp_user is required and must be a string')
            if type(config_obj.get('ftp_port', 21)) != int:
                raise ValueError('ftp_port must be an integer')
            if type(config_obj.get('ftp_timeout', 20)) != int:
                raise ValueError('ftp_timeout must be an integer')
            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')
            if type(config_obj.get('update_all', False)) != bool:
                raise ValueError('update_all must be true or false')
            if type(config_obj.get('max_dataset', 100)) != int:
                raise ValueError('max_dataset must be an integer')
        except ValueError as e:
            raise e

        return config

    # Required by NextGEOSS base harvester
    def gather_stage(self, harvest_job):
        self.log = logging.getLogger(__file__)
        self.log.debug('FSSCAT Harvester gather_stage for job: %r', harvest_job)

        self.source_config = self._get_config(harvest_job)
        max_datasets = self.source_config.get('max_dataset', 100)
        ftp_info = {
            "domain": self.source_config.get('ftp_domain'),
            "path":  self.source_config.get('ftp_path'),
            "username": self.source_config.get('ftp_user'),
            "password": self.source_config.get('ftp_pass'),
            "port": self.source_config.get('ftp_port', 21),
            "timeout": self.source_config.get('ftp_timeout', 20)
        }
        self.update_all =  self.source_config.get('update_all', False)

        last_product_date = (
            self._get_last_harvesting_date(harvest_job.source_id)
        )
        if last_product_date is not None:
            start_date = last_product_date + timedelta(days=1)
            start_date = start_date.strftime("%Y-%m-%d")
        else:
            start_date =  self.source_config.get('start_date')

        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date =  self.source_config.get('end_date', False)
        end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()  # noqa: E501

        ftp_source = create_ftp_source(ftp_info)

        products = ftp_source.get_products_path(start_date, end_date,
                                                max_datasets)

        ids = []
        for product in products:
            resources = ftp_source.get_resources_url(product)
            manifest_str = "manifest.fssp.safe.xml"
            manifest_url = [resource for resource in resources if 
                            manifest_str in resource]
            if manifest_url:
                manifest_url = manifest_url[0]
                manifest_content = ftp_source.get_file_content(manifest_url)

            ids.append(self._gather_object(harvest_job, product, resources,
                                           manifest_content))

        return ids

    def fetch_stage(self, harvest_object):
        return True

    def _get_last_harvesting_date(self, source_id):
        """
        Return the ingestion date of the last product harvested or none
        if no previous harvesting job
        """
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
                            guid=unicode(uuid.uuid4()),
                            extras=extras,
                            content=content)
        obj.package = package
        obj.save()
        return obj.id


def create_ftp_source(ftp_info):
    return FtpSource(**ftp_info)

class FtpSource(object):

    def __init__(self, domain, path, username, password, port, timeout):
        self.domain = domain
        self.path = path
        self.username = username
        self.password = password
        self.port = port
        self.timeout = float(timeout)

    def get_products_path(self, start_date, end_date, max_datasets):
        """
        Parse the FTP contrained by time interval and maximum datasets
        and return a list of product URL
        """
        ftp_urls = set()
        ftp = self._connect_ftp()
        ftp_path_exists = self._check_ftp_path(ftp, self._get_ftp_path())
        if not ftp_path_exists:
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

            # Assumption: date_path follows the structure "/Year/month/day"
            # inverse_depth stores:
            # 0 - found ftp path corresponding to date_path
            # 1 - missing the "day" folder in the ftp
            # 2 - missing the "month" folder in the ftp
            # 3 - missing the "year" folder in the ftp
            inverse_depth = self._check_ftp_date_path(ftp, path_list)

            if inverse_depth:
                # Depth inverted to represent 
                fail_depth = path_list_len - inverse_depth
                harvest_date = self._get_new_harvest_date(harvest_date, fail_depth)
                if not harvest_date:
                    ftp.quit()
                    return ftp_urls

            else:
                products_list = self._get_product_list_from_file(harvest_date)
                if products_list:
                    new_product_count = products_count + len(products_list)
                    _to_harvest = self._to_harvest(harvest_date, start_date,
                                                end_date, new_product_count,
                                                max_datasets)
                    # Check if the number of products in the current folder plus
                    # already collected is larger than max_datasets
                    # NOTE: Rule ignored if no product has been collected 
                    _to_harvest = _to_harvest or products_count == 0
                    if _to_harvest:
                        ftp_urls |= set(product for product in products_list)
                        harvest_date += timedelta(days=1)
                        products_count = len(ftp_urls)
                    else:
                        ftp.quit()
                        return ftp_urls
                else:
                    harvest_date += timedelta(days=1)
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
        """
        Check if the date path is in the ftp.
        Returns 0 if path exists, else returns the depth at which
        the folder was not found.
        The depth starts at the most granular (day)
        """
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

    def _check_ftp_path(self, ftp, path):
        """
        Check if the path exists within the ftp.
        """
        path_list = path.strip("/").split("/")
        for _path in path_list:
            if _path in ftp.nlst():
                ftp.cwd(_path)
            else:
                return False
        ftp.cwd("/")
        return True

    def _to_harvest(self, date, start, end, products_count, max_products):
        """
        Check the harvesting conditions are being met.
        Time contraint: harvesting date within time interval
        Size constraint: the number of products collected is less than the maximum
        allowed
        """
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

    def get_resources_url(self, ftp_path):
        """
        Retrieves the list of all ftp url for all files and folders 
        found on the ftp path
        """
        resources = set()
        ftp = self._connect_ftp()
        ftp.cwd(ftp_path)
        resources |=set(self._ftp_url(ftp_path, resource_url)
                        for resource_url in ftp.nlst())
        ftp.quit()
        return resources

    def get_file_content(self, file_url):
        """
        Retrieve ftp file content into string via stream
        """
        ftp = self._connect_ftp()
        ftp_path = self._ftp_path_from_url(file_url)
        r = StringIO()
        ftp.retrbinary('RETR {}'.format(ftp_path), r.write)
        ftp.quit()
        return r.getvalue()

    def _connect_ftp(self):
        ftp = FTP()
        ftp.connect(self.domain,self.port,self.timeout)
        ftp.login(self.username, self.password)

        return ftp

    def _get_file_list_name(self, date):
        filename_template = "{}_file_list.txt"
        filename = filename_template.format(date.strftime("%Y%m%d"))
        return filename

    def _get_product_list_from_file(self, date, ftp):
        filename = self._get_file_list_name(date)
        date_path = self._date_path(date)
        ftp_path =self._ftp_path(date_path, "")
        if filename in ftp.nlst():
            file_url = self._ftp_url(ftp_path , filename)
            file_content = self.get_file_content(file_url)
            return file_content.split()
        else:
            warning_msg = "File {} does could not be found at {}. Skipping it!"
            log.warning(warning_msg.format(filename, ftp_path))
            return None