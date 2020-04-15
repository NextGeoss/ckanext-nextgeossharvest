# -*- coding: utf-8 -*-

import logging
import json
from datetime import datetime

from sqlalchemy import desc

from ckan.common import config
from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.esa_base import SentinelHarvester
from ckanext.nextgeossharvest.lib.opensearch_base import OpenSearchHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester


class ESAHarvester(SentinelHarvester, OpenSearchHarvester, NextGEOSSHarvester):
    """A Harvester for ESA Sentinel Products."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'esasentinel',
            'title': 'ESA Sentinel Harvester New',
            'description': 'A Harvester for ESA Sentinel Products'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if config_obj.get('source') not in {'esa_scihub', 'esa_noa', 'esa_code'}:  # noqa: E501
                raise ValueError('source is required and must be either scihub or noa')  # noqa: E501
            if 'start_date' in config_obj:
                try:
                    datetime.strptime(config_obj['start_date'],
                                      '%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError:
                    raise ValueError('start_date format must be 2018-01-01T00:00:00.000Z')  # noqa: E501
            if 'end_date' in config_obj:
                try:
                    datetime.strptime(config_obj['end_date'],
                                      '%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError:
                    raise ValueError('end_date format must be 2018-01-01T00:00:00.000Z')  # noqa: E501
            if 'datasets_per_job' in config_obj:
                limit = config_obj['datasets_per_job']
                if not isinstance(limit, int) and not limit > 0:
                    raise ValueError('datasets_per_job must be a positive integer')  # noqa: E501
            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                if not isinstance(timeout, int) and not timeout > 0:
                    raise ValueError('timeout must be a positive integer')
            for key in ['update_all', 'skip_raw', 'multiple_sources']:
                if key in config_obj and not isinstance(config_obj[key], bool):
                    raise ValueError('{} must be boolean'.format(key))
            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')

        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.ESASentinel.gather')
        log.debug('ESASentinelHarvester gather_stage for job: %r', harvest_job)

        # Save a reference
        self.job = harvest_job

        self._set_source_config(self.job.source.config)

        self.update_all = self.source_config.get('update_all', False)

        # If we need to restart, we can do so from the ingestion timestamp
        # of the last harvest object for the source. So, query the harvest
        # object table to get the most recently created harvest object
        # and then get its restart_date extra, and use that to restart
        # the queries
        last_object = Session.query(HarvestObject). \
            filter(HarvestObject.harvest_source_id == self.job.source_id,
                   HarvestObject.import_finished != None). \
            order_by(desc(HarvestObject.import_finished)).limit(1)  # noqa: E711, E501
        if last_object:
            try:
                last_object = last_object[0]
                restart_date = self._get_object_extra(last_object,
                                                      'restart_date', '*')
            except IndexError:
                restart_date = '*'
        else:
            restart_date = '*'
        log.debug('Restart date is {}'.format(restart_date))

        if restart_date == '*':
            start_date = self.source_config.get('start_date', restart_date)
        else:
            start_date = restart_date

        end_date = self.source_config.get('end_date', 'NOW')
        date_range = '[{} TO {}]'.format(start_date, end_date)

        # Get the base_url
        source = self.source_config.get('source')
        if source == 'esa_scihub':
            base_url = 'https://scihub.copernicus.eu'
            self.os_id_name = 'str',
            self.os_id_attr = {'name': 'identifier'}
            self.os_guid_name = 'str'
            self.os_guid_attr = {'name': 'uuid'}
            self.os_restart_date_name = 'date'
            self.os_restart_date_attr = {'name': 'ingestiondate'}
            self.os_restart_filter = None
            self.flagged_extra = 'scihub_download_url'
        elif source == 'esa_noa':
            base_url = 'https://sentinels.space.noa.gr'
            self.os_id_name = 'str',
            self.os_id_attr = {'name': 'identifier'}
            self.os_guid_name = 'str'
            self.os_guid_attr = {'name': 'uuid'}
            self.os_restart_date_name = 'date'
            self.os_restart_date_attr = {'name': 'ingestiondate'}
            self.os_restart_filter = None
            self.flagged_extra = 'noa_download_url'
        elif source == 'esa_code':
            base_url = 'https://code-de.org'
            self.os_id_name = 'str',
            self.os_id_attr = {'name': 'identifier'}
            self.os_guid_name = 'str'
            self.os_guid_attr = {'name': 'uuid'}
            self.os_restart_date_name = 'date'
            self.os_restart_date_attr = {'name': 'ingestiondate'}
            self.os_restart_filter = None
            self.flagged_extra = 'code_download_url'

        if self.source_config.get('skip_raw'):
            skip_raw = ' AND NOT producttype:RAW'
        else:
            skip_raw = ''

        limit = self.source_config.get('datasets_per_job', 100)

        url_template = ('{base_url}/dhus/search?' +
                        'q=ingestiondate:{date_range}' +
                        '{skip_raw}' +
                        '&orderby=ingestiondate asc' +
                        '&start=0' +
                        '&rows={limit}')
        harvest_url = url_template.format(base_url=base_url,
                                          date_range=date_range,
                                          skip_raw=skip_raw, limit=limit)
        log.debug('Harvest URL is {}'.format(harvest_url))
        username = config.get('ckanext.nextgeossharvest.nextgeoss_username')
        password = config.get('ckanext.nextgeossharvest.nextgeoss_password')

        # Set the limit for the maximum number of results per job.
        # Since the new harvester jobs will be created on a rolling basis
        # via cron jobs, we don't need to grab all the results from a date
        # range at once and the harvester will resume from the last gathered
        # date each time it runs.
        timeout = self.source_config.get('timeout', 4)

        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()
        self.provider = source

        # This can be a hook
        ids = self._crawl_results(harvest_url, limit, timeout, username,
                                  password)
        # This can be a hook

        return ids

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True
