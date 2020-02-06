# -*- coding: utf-8 -*-
import re
import os

import json
import logging
from datetime import datetime, timedelta
from monthdelta import monthdelta
from ckan.plugins.core import implements
from ckan.model import Session
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.nextgeossharvest.lib.gdacs_base import GDACSBase
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from sqlalchemy import desc
from bs4 import BeautifulSoup
import requests


def parse_filename(url):
    fname = url.split('/')[-1]
    return os.path.splitext(fname)[0]


class GDACSHarvester(NextGEOSSHarvester, GDACSBase):
    '''
    A Harvester for GDACS Average Flood Data.
    '''
    implements(IHarvester)

    def __init__(self, *args, **kwargs):
        super(type(self), self).__init__(*args, **kwargs)
        self.overlap = timedelta(days=30)
        self.interval = timedelta(days=3 * 30)

    def info(self):
        return {
            'name': 'gdacs',
            'title': 'GDACS',
            'description': 'A Harvester for GDACS Average Flood Data.'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if config_obj.get('data_type') not in {'signal', 'magnitude'}:
                raise ValueError('data_type is required and must be "signal" or "magnitude"')  # noqa: E501
            if config_obj.get('request_check') not in {'yes', 'no'}:
                raise ValueError('request_check is required and must be "yes" or "no"')  # noqa: E501
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
            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')
        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        self.log = logging.getLogger(__file__)
        self.log.debug('GDACS Harvester gather_stage for job: %r', harvest_job)
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
        self.provider = 'gdacs'
        self.job = harvest_job
        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()
        if not hasattr(self, 'harvester_logger'):
            self.harvester_logger = self.make_harvester_logger()
        ids = (
            self._gather(harvest_job,
                         start_date, end_date, harvest_job.source_id, config)
        )
        return ids

    def _gather(self, job, start_date, end_date, source_id, config):
        data_type = config['data_type']
        request_check = config['request_check']
        http_source = create_http_source(data_type)
        existing_files = (
            http_source._get_http_urls(start_date, end_date)
        )
        harvested_files = self._get_ckan_guids(start_date, end_date, source_id)
        non_harvested_files = existing_files - harvested_files
        ids = []
        for http_url in non_harvested_files:
            if request_check == 'yes':
                status_code = self._crawl_urls_http(http_url, self.provider)
            else:
                status_code = 200
            if status_code == 200:
                start_date = http_source.parse_date(http_url)
                assert start_date
                ids.append(self._gather_object(job, http_url, start_date))
        harvester_msg = '{:<12} | {} | jobID:{} | {} | {}'
        if hasattr(self, 'harvester_logger'):
            timestamp = str(datetime.utcnow())
            self.harvester_logger.info(harvester_msg.format(self.provider, timestamp, self.job.id, len(non_harvested_files), 0))  # noqa: E128, E501
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

    def _gather_object(self, job, url, start_date):
        filename = parse_filename(url)
        filename_id = filename
        extras = [HOExtra(key='status', value='new')]
        assert start_date
        content = json.dumps({
            'identifier': filename_id,
            'http_link': url,
            'start_date': start_date,
            'restart_date': start_date
        }, default=str
        )
        obj = HarvestObject(job=job,
                            guid=url,
                            extras=extras,
                            content=content)
        obj.save()
        return obj.id


def create_http_source(data_type):
    return HttpSource(**HTTP_SOURCE_CONF[data_type])


class HttpSource(object):

    def __init__(self, domain, path, fname_pattern, date_dir=True):
        self.fname_pattern = re.compile(fname_pattern)
        self.domain = domain
        self.path = path
        self.date_dir = date_dir

    def _get_http_urls(self, start_date, end_date):
        http_urls = []
        http_url = self._get_http_domain()
        http_url_complete = self._get_http_domain() + '/' + self._get_http_path() + '/'  # noqa: E501
        ext = "tif"
        for directory in self._get_http_directories(start_date, end_date):
            http_url_date = http_url_complete + directory
            page = requests.get(http_url_date).text
            soup = BeautifulSoup(page, 'html.parser')
            dir_list = [http_url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]  # noqa: E501
            http_urls = http_urls + dir_list
        http_urls = set(http_urls)
        return http_urls

    def _to_harvest(self, fname, start_date, end_date):
        date = self._parse_date(fname)
        return date >= start_date and date <= end_date

    def _get_http_domain(self):
        return self.domain

    def _get_http_path(self):
        return self.path

    def _get_http_directories(self, start_date, end_date):
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
        date_str = self.fname_pattern.match(filename + '.tif').group('date')
        date = datetime.strptime(date_str, '%Y%m%d')
        return date

# Configuration for the harvester providers:
# '<data_type as in config>': {
#   'domain': url domain of the source,
#   'path': the path where all the files to be collected are,
#   'fname_pattern': a regex expression to match the name of all file names
#   'data_dir': specifies if the files are structured in directories by date
# }


HTTP_SOURCE_CONF = {
    'signal': {
        'domain': 'http://www.gdacs.org',
        'path': 'flooddetection/DATA/ALL/AvgSignalTiffs',
        'fname_pattern': r'signal_4days_avg_4days_(?P<date>\d{8,8}).tif',
    },
    'magnitude': {
        'domain': 'http://www.gdacs.org',
        'path': 'flooddetection/DATA/ALL/AvgMagTiffs',
        'fname_pattern': r'mag_4days_signal_4days_avg_4days_(?P<date>\d{8,8}).tif',  # noqa: E501
    }

}
