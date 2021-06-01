# -*- coding: utf-8 -*-
import re
import os

import json
import logging
from datetime import datetime, timedelta
from monthdelta import monthdelta

from ftplib import FTP

from ckan.plugins.core import implements
from ckan.model import Session


from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.nextgeossharvest.lib.cmems_base import CMEMSBase
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObjectExtra as HOExtra

from sqlalchemy import desc


def parse_filename(url):
    fname = url.split('/')[-1]
    return os.path.splitext(fname)[0]


class CMEMSHarvester(NextGEOSSHarvester, CMEMSBase):
    '''
    A Harvester for CMEMS Products.
    '''
    implements(IHarvester)

    def __init__(self, *args, **kwargs):
        super(type(self), self).__init__(*args, **kwargs)
        self.overlap = timedelta(days=30)
        self.interval = timedelta(days=3 * 30)

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
                                                        'gpaf',
                                                        'mog',
                                                        'bs_sst_006'}:
                raise ValueError('harvester type is required and must be "sst" or "sic_north" or "sic_south" or "ocn" or "slv" or "gpaf" or "mog" or "bs_sst_006"')  # noqa: E501
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
                end_date = self.convert_date_config('TODAY')
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
            if type(config_obj.get('update_all', False)) != bool:
                raise ValueError('update_all must be true or false')
        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        self.log = logging.getLogger(__file__)
        self.log.debug('CMEMS Harvester gather_stage for job: %r', harvest_job)
        config = self._get_config(harvest_job)
        last_product_date = (
            self._get_last_harvesting_date(harvest_job.source_id)
        )
        if last_product_date is not None:
            start_date = last_product_date
        else:
            start_date = self._parse_date(config['start_date'])
        end_date = min(start_date + self.interval,
                       datetime.now(),
                       self._parse_date(
                           config.get('end_date')
                           if config.get('end_date') is not None
                           else
                           self.convert_date_config(
                               'TODAY').strftime("%Y-%m-%d")))
        ids = (
            self._gather(harvest_job,
                         start_date, end_date, harvest_job.source_id, config)
        )
        return ids

    def _gather(self, job, start_date, end_date, source_id, config):
        ftp_user = config['username']
        ftp_passwd = config['password']
        source_type = config['harvester_type']
        ftp_source = create_ftp_source(source_type)
        existing_files = (
            ftp_source._get_ftp_urls(start_date,
                                     end_date, ftp_user, ftp_passwd)
        )
        self.update_all = config.get('update_all', False)
        harvested_files = self._get_ckan_guids(start_date, end_date, source_id)
        non_harvested_files = existing_files - harvested_files
        ids = []
        for ftp_url in non_harvested_files:
            size = 0
            start_date = ftp_source.parse_date(ftp_url)
            assert start_date
            forecast_date = ftp_source.parse_forecast_date(ftp_url)
            ids.append(self._gather_object(job,
                                           ftp_url, size,
                                           start_date, forecast_date))
        return ids

    def fetch_stage(self, harvest_object):
        return True

    def _get_ckan_guids(self, start_date, end_date, source_id):
        objects = self._get_imported_harvest_objects_by_source(source_id)
        return set(obj.guid for obj in objects)

    def _get_last_harvesting_date(self, source_id):
        objects = self._get_imported_harvest_objects_by_source(source_id)
        sorted_objects = objects.order_by(desc(HarvestObject.import_finished))
        last_object = sorted_objects.limit(1).first()
        if last_object is not None:
            restart_date = json.loads(last_object.content)['restart_date']
            return datetime.strptime(restart_date, '%Y-%m-%d %H:%M:%S')
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
            if date_str != 'TODAY' and date_str != 'YESTERDAY':
                return datetime.strptime(date_str, '%Y-%m-%d')
            else:
                return self.convert_date_config(date_str)
        else:
            return None

    def _gather_object(self, job, url, size, start_date, forecast_date):
        filename = parse_filename(url)
        filename_id = (
            filename.replace('-v02.0-fv02.0', '').replace('-fv02.0', '')
        )

        status, package = self._was_harvested(filename_id, self.update_all)

        extras = [HOExtra(key='status', value=status)]
        assert start_date
        content = json.dumps({
            'identifier': filename_id,
            'ftp_link': url,
            'size': size,
            'start_date': start_date,
            'forecast_date': forecast_date,
            'restart_date': start_date
        }, default=str
        )
        obj = HarvestObject(job=job,
                            guid=url,
                            extras=extras,
                            content=content)
        obj.package = package
        obj.save()
        return obj.id


def create_ftp_source(source_type):
    return FtpSource(**FTP_SOURCE_CONF[source_type])


class FtpSource(object):

    def __init__(self, domain, path, fname_pattern, date_dir=True):
        self.fname_pattern = re.compile(fname_pattern)
        self.domain = domain
        self.path = path
        self.date_dir = date_dir

    def _get_ftp_urls(self, start_date, end_date, user, passwd):
        ftp_urls = set()
        ftp = FTP(self._get_ftp_domain(), user, passwd)
        for directory in self._get_ftp_directories(start_date, end_date):
            ftp.cwd('/{}/{}'.format(self._get_ftp_path(), directory))
            ftp_urls |= set(self._ftp_url(directory, fname)
                            for fname in ftp.nlst()
                            if self.fname_pattern.match(
                                fname) and self._to_harvest(
                                    fname, start_date, end_date)
                            )
        return ftp_urls

    def _to_harvest(self, fname, start_date, end_date):
        date = self._parse_date(fname)
        return date >= start_date and date <= end_date

    def _ftp_url(self, directory, filename):
        return 'ftp://{}/{}/{}/{}'.format(self._get_ftp_domain(),
                                          self._get_ftp_path(),
                                          directory,
                                          filename)

    def _get_ftp_domain(self):
        return self.domain

    def _get_ftp_path(self):
        return self.path

    def _get_ftp_directories(self, start_date, end_date):
        if self.date_dir:
            directories = []
            date = start_date
            while date <= end_date:
                directories.append(self._date_path(date))
                date += monthdelta(1)
            if (self._date_path(date) == self._date_path(end_date)) and \
                    (self._date_path(date) not in directories):
                directories.append(self._date_path(date))
            return directories
        else:
            return ['/']

    def _date_path(self, date):
        return date.strftime('%Y/%m')

    def parse_date(self, ftp_url):
        filename = parse_filename(ftp_url)
        return self._parse_date(filename)

    def _parse_date(self, filename):
        date_str = self.fname_pattern.match(filename + '.nc').group('date')
        date = datetime.strptime(date_str, '%Y%m%d')
        return date

    def parse_forecast_date(self, ftp_url):
        filename = parse_filename(ftp_url)
        try:
            date_str = (
                self.fname_pattern.match(
                    filename + '.nc'
                ).group('forecast_date')
            )
            return datetime.strptime(date_str, '%Y%m%d')
        except IndexError:
            return None

# Configuration for the harvester providers:
# '<harvester_type as in config>': {
#   'domain': url domain of the source,
#   'path': the path where all the files to be collected are,
#   'fname_pattern': a regex expression to match the name of all file names
#   'data_dir': specifies if the files are structured in directories by date
# }


FTP_SOURCE_CONF = {
    'sst': {
        'domain': 'nrt.cmems-du.eu',
        'path': 'Core/SST_GLO_SST_L4_NRT_OBSERVATIONS_010_001/'
        'METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2',
        'fname_pattern': r'(?P<date>\d{8,8})120000-UKMO-L4_GHRSST-SSTfnd-OSTIA'
        '-GLOB-v02.0-fv02.0.nc',
    },
    'sic_north': {
        'domain': 'mftp.cmems.met.no',
        'path': 'Core/SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/'
        'METNO-GLO-SEAICE_CONC-NORTH-L4-NRT-OBS',
        'fname_pattern': r'ice_conc_nh_ease-125_multi_'
        '(?P<date>\d{8,8})1200.nc',
    },
    'sic_south': {
        'domain': 'mftp.cmems.met.no',
        'path': 'Core/SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/'
        'METNO-GLO-SEAICE_CONC-SOUTH-L4-NRT-OBS',
        'fname_pattern': r'ice_conc_sh_ease-125_multi_'
        '(?P<date>\d{8,8})1200.nc',
    },
    'ocn': {
        'domain': 'mftp.cmems.met.no',
        'path': 'Core/ARCTIC_ANALYSIS_FORECAST_PHYS_002_001_a/'
        'dataset-topaz4-arc-myoceanv2-be',
        'fname_pattern': r'(?P<forecast_date>\d{8,8})_dm-metno-MODEL'
        '-topaz4-ARC-b(?P<date>\d{8,8})-fv02.0.nc',
        'date_dir': False,
    },
    'slv': {
        'domain': 'nrt.cmems-du.eu',
        'path': 'Core/SEALEVEL_GLO_PHY_L4_NRT_OBSERVATIONS_008_046/'
        'dataset-duacs-nrt-global-merged-allsat-phy-l4',
        'fname_pattern': r'nrt_global_allsat_phy_l4_'
        '(?P<date>\d{8,8})_\d{8,8}.nc',
    },
    'gpaf': {
        'domain': 'nrt.cmems-du.eu',
        'path': 'Core/GLOBAL_ANALYSIS_FORECAST_PHY_001_024/'
        'global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh',
        'fname_pattern': r'mercatorpsy4v3r1_gl12_hrly_'
        '(?P<forecast_date>\d{8,8})_R(?P<date>\d{8,8}).nc',
    },
    'mog': {
        'domain': 'nrt.cmems-du.eu',
        'path': 'Core/MULTIOBS_GLO_PHY_NRT_015_003/dataset-uv-nrt-hourly',
        'fname_pattern': r'dataset-uv-nrt-hourly_(?P<date>\d{8,8})T0000Z'
        '_P\d{8,8}T\d{4}.nc',
    },
    'bs_sst_006': {
        'domain': 'nrt.cmems-du.eu',
        'path': 'Core/SST_BS_SST_L4_NRT_OBSERVATIONS_010_006/SST_BS_SST_L4_NRT_OBSERVATIONS_010_006_c_V2',
        'fname_pattern': r'(?P<date>\d{8,8})000000-GOS-L4_GHRSST-SSTfnd-OISST_UHR_NRT-BLK-v02.0-fv02.0.nc',
    }

}
