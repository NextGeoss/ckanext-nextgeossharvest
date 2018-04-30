# -*- coding: utf-8 -*-
""" The GOME2 harvester."""

import json
import logging
from datetime import datetime

from ckan.plugins.core import implements

from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.gome2_base import GOME2Base
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester


class GOME2Harvester(GOME2Base,
                     NextGEOSSHarvester):
    '''
    A Harvester for GOME2 Products.
    '''
    implements(IHarvester)

    def info(self):
        return {
            'name': 'gome2',
            'title': 'GOME2',
            'description': 'A Harvester for GOME2 Products'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'start_date' in config_obj:
                try:
                    start_date = datetime.strptime(config_obj['start_date'],
                                                   '%Y-%m-%d')
                except ValueError:
                    raise ValueError('start_date format must be yyyy-mm-dd')
            else:
                raise ValueError('start_date is required')
            if 'end_date' in config_obj:
                try:
                    end_date = datetime.strptime(config_obj['end_date'],
                                                 '%Y-%m-%d')
                except ValueError:
                    raise ValueError('end_date format must be yyyy-mm-dd')
            else:
                raise ValueError('end_date is required')
            if not end_date >= start_date:
                raise ValueError('end_date must be >= start_date')
            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')
        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.gather')
        log.debug('GOME2 Harvester gather_stage for job: %r', harvest_job)

        self.job = harvest_job
        self._set_source_config(harvest_job.source.config)

        start_date = self.source_config.get('start_date')
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')

        end_date = self.source_config.get('end_date', 'NOW')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')

        ids = self._create_harvest_objects()

        return ids

    def fetch_stage(self, harvest_object):
        return True
