# -*- coding: utf-8 -*-

import json
import logging
from datetime import datetime

from ckan.plugins.core import implements

from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.cmems_base import CMEMSBase
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester


class CMEMSHarvester(CMEMSBase,
                     NextGEOSSHarvester):
    '''
    A Harvester for CMEMS Products.
    '''
    implements(IHarvester)

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

            if config_obj.get('harvester_type') not in {'sst', 'sic_north', 'sic_south', 'ocn'}:  # noqa: E501
                raise ValueError('harvester type is required and must be "sst" or "sic_north" or "sic_south" or "ocn"')  # noqa: E501
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
        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.gather')
        log.debug('CMEMS Harvester gather_stage for job: %r', harvest_job)

        self.job = harvest_job
        self._set_source_config(harvest_job.source.config)
        self.harvester_type = self.source_config['harvester_type']

        start_date = self.source_config.get('start_date')
        if start_date == 'YESTERDAY':
            start_date = self.convert_date_config(start_date)
        else:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.start_date = start_date

        end_date = self.source_config.get('end_date', 'NOW')
        if end_date in {'TODAY', 'NOW'}:
            end_date = self.convert_date_config(end_date)
        else:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.end_date = end_date

        ids = self._get_metadata_create_objects()

        return ids

    def fetch_stage(self, harvest_object):
        return True
