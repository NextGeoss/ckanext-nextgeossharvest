# -*- coding: utf-8 -*-

import json
import logging
from datetime import datetime, timedelta


from ftplib import FTP

from ckan.plugins.core import implements
from ckan.model import Session


from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.nextgeossharvest.lib.cmems_base import CMEMSBase
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObjectExtra as HOExtra

from sqlalchemy import desc

from ftplib import all_errors as FtpException


def parse_filename(url):
    return url.split('/')[-1].split('.')[0]

class CMEMSHarvester(NextGEOSSHarvester, CMEMSBase):
    '''
    A Harvester for CMEMS Products.
    '''
    implements(IHarvester)

    def __init__(self, *args, **kwargs):
        super(type(self), self).__init__(*args, **kwargs)
        self.overlap = timedelta(days=30)
        self.interval = timedelta(days=3*30)

    def info(self):
        return {
            'name': 'cmems',
            'title': 'CMEMS',
            'description': 'A Harvester for CMEMS Products'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if config_obj.get('harvester_type') not in {'sst',
                                                        'sic_north',
                                                        'sic_south',
                                                        'ocn',
                                                        'slv',
                                                        'gpaf'}:
                raise ValueError('harvester type is required and must be "sst" or "sic_north" or "sic_south" or "ocn" or "slv" or "gpaf"')  # noqa: E501
            if 'start_date' in config_obj:
                try:
                    start_date = config_obj['start_date']
                    if start_date != 'YESTERDAY':
                        start_date = datetime.strptime(start_date, '%Y-%m-%d')
                    else:
                        start_date = self.convert_date_config(start_date)
                except ValueError:
                    raise ValueError('start_date format must be yyyy-mm-dd')
            else:
                raise ValueError('start_date is required')
            if 'end_date' in config_obj:
                try:
                    end_date = config_obj['end_date']
                    if end_date != 'TODAY':
                        end_date = datetime.strptime(end_date, '%Y-%m-%d')
                    else:
                        end_date = self.convert_date_config(end_date)
                except ValueError:
                    raise ValueError('end_date format must be yyyy-mm-dd')
            else:
                raise ValueError('end_date is required')
            if not end_date > start_date:
                raise ValueError('end_date must be after start_date')
            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                if not isinstance(timeout, int) and not timeout > 0:
                    raise ValueError('timeout must be a positive integer')
            if type(config_obj.get('password', None)) != unicode:
                raise ValueError('password is required and must be a string')
            if type(config_obj.get('username', None)) != unicode:
                raise ValueError('username is requred and must be a string')
            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')
        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        self.log = logging.getLogger(__file__)
        self.log.debug('CMEMS Harvester gather_stage for job: %r', harvest_job)
        config = self._get_config(harvest_job)
        last_product_date = self._get_last_harvesting_date(harvest_job.source_id)
        if last_product_date is not None:
            start_date = last_product_date
        else:
            start_date = self._parse_date(config['start_date'])
        end_date = min(start_date + self.interval,
                       datetime.now(),
                       self._parse_date(config.get('end_date')))
        ids = self._gather(harvest_job, start_date, end_date, harvest_job.source_id, config)
        print('ids', ids)
        return ids

    def _gather(self, job, start_date, end_date, source_id, config):
        ftp_user = config['username']
        ftp_passwd = config['password']
        source = config['harvester_type']
        ftp_source = FtpSource()
        existing_files = self._get_ftp_urls(start_date, end_date, ftp_source, ftp_user, ftp_passwd)
        print('Found in FTP: %s', existing_files)
        harvested_files = self._get_ckan_guids(start_date, end_date, source_id)
        print('harvest files', harvested_files)
        non_harvested_files = existing_files - harvested_files
        print('non harvested', non_harvested_files)
        ids = []
        for ftp_url in non_harvested_files:
            size = 0
            ids.append(self._gather_object(job, ftp_url, size, ftp_source))
        return ids

    def fetch_stage(self, harvest_object):
        return True

    def _get_ckan_guids(self, start_date, end_date, source_id):
        objects = self._get_imported_harvest_objects_by_source(source_id)
        return set(obj.guid for obj in objects)

    def _get_last_harvesting_date(self, source_id):
        objects = self._get_imported_harvest_objects_by_source(source_id)
        sorted_objets = objects.order_by(desc(HarvestObject.import_finished))
        last_object = sorted_objets.limit(1).first()
        if last_object is not None:
            restart_date = self._get_object_extra(last_object,
                                                  'restart_date')
            restart_date = '2017-10-01' 
            return datetime.strptime(restart_date, '%Y-%m-%d')
        else:
            return None

    def _get_imported_harvest_objects_by_source(self, source_id):
        return Session.query(HarvestObject).filter(
            HarvestObject.harvest_source_id == source_id,
            HarvestObject.import_finished is not None)

    def _get_config(self, harvest_job):
        return json.loads(harvest_job.source.config)

    def _parse_date(self, date_str):
        if date_str:
            return datetime.strptime(date_str, '%Y-%m-%d')
        else:
            return None

    def _get_ftp_urls(self, start_date, end_date, ftp_source, user, passwd):
        ftp_urls = set()
        ftp = FTP(ftp_source._get_ftp_domain(), user, passwd)
        ftp.cwd(ftp_source._get_ftp_path())
        for directory in ftp_source._get_ftp_directories():
            ftp.cwd(directory)
            ftp_urls |= set(ftp_source._ftp_url(directory, fname) for fname in ftp.nlst())
        return ftp_urls

    def _gather_object(self, job, url, size, ftp_source):
        filename = parse_filename(url)
        print('gathering %s', filename)
        extras = [HOExtra(key='status', value='new')]
        content = json.dumps({
                'identifier': filename,
                'ftp_link': url,
                'size': size,
                'start_date': ftp_source.parse_date(url),
                'forecast_date': ftp_source.parse_forecast_date(url)
            },
            default=str)
        obj = HarvestObject(job=job, 
                            guid=url,
                            extras=extras, 
                            content=content)
        obj.save()
        return obj.id

class FtpSource(object):

    def _ftp_url(self, directory, filename):
        return 'ftp://{}/{}/{}/{}'.format(self._get_ftp_domain(), 
                                          self._get_ftp_path(), 
                                          directory, 
                                          filename)

    def _get_ftp_domain(self):
        return 'nrt.cmems-du.eu'

    def _get_ftp_path(self):
        return 'Core/SEALEVEL_GLO_PHY_L4_NRT_OBSERVATIONS_008_046/dataset-duacs-nrt-global-merged-allsat-phy-l4'

    def _get_ftp_directories(self):
        return ['2017/01']

    def parse_date(self, ftp_url):
        filename = parse_filename(ftp_url)
        filename_parts = filename.split('_')
        date_str = filename_parts[5]
        date = datetime.strptime(date_str, '%Y%m%d')
        return date

    def parse_forecast_date(self, ftp_url):
        return None
   
    
   
class SlvFtpSource(FtpSource):
    pass
