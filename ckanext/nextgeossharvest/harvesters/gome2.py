# -*- coding: utf-8 -*-
""" The GOME2 harvester."""

import json
import logging
from datetime import datetime, timedelta

from ckan.plugins.core import implements

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.interfaces import IHarvester
from ckanext.nextgeossharvest.lib.gome2_base import GOME2Base
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckan.model import Session
from sqlalchemy import desc


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

            start_date = config_obj['start_date']

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

            if not (end_date > start_date) or (start_date == 'YESTERDAY' and end_date == 'TODAY'):  # noqa: E501
                raise ValueError('end_date must be > start_date')

            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')
        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.gather')
        log.debug('GOME2 Harvester gather_stage for job: %r', harvest_job)

        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()

        self.job = harvest_job
        self._set_source_config(harvest_job.source.config)

        start_date = self.source_config.get('start_date')
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')

        end_date = self.source_config.get('end_date', 'NOW')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')

        if self.get_last_harvesting_date() == '*':
           self.start_date = self.start_date

        else:
            self.start_date = self.get_last_harvesting_date()
            

        date = self.start_date
        date_strings = []
        while date < self.end_date:
            date_strings.append(datetime.strftime(date, '%Y-%m-%d'))
            date += timedelta(days=1)
        self.date_strings = date_strings

        ids = self._create_harvest_objects()

        return ids

    def fetch_stage(self, harvest_object):
        return True

    def get_last_harvesting_date(self):
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
        return restart_date