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

from ckanext.nextgeossharvest.lib.plan4all_base import OLUHarvester
from ckanext.nextgeossharvest.lib.cswsearch_base import CSWSearchHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester


class Plan4AllHarvester(OLUHarvester, CSWSearchHarvester, NextGEOSSHarvester):
    """A Harvester for ESA Sentinel Products."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'plan4all',
            'title': 'Plan4All Harvester',
            'description': 'A Harvester for OLU Products'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)
            
            if 'datasets_per_job' in config_obj:
                limit = config_obj['datasets_per_job']
                if not isinstance(limit, int) and not limit > 0 and not limit < 101:
                    raise ValueError('datasets_per_job must be a positive integer less than 100')  # noqa: E501
            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                if not isinstance(timeout, int) and not timeout > 0:
                    raise ValueError('timeout must be a positive integer')
            key ='update_all'
            if key in config_obj:
                if not isinstance(config_obj[key], bool):
                    raise ValueError('{} must be boolean'.format(key))
                
            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')

        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.Plan4All.gather')
        log.debug('OLUHarvester gather_stage for job: %r', harvest_job)

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
                restart_record = self._get_object_extra(last_object,
                                                      'restart_record', '1')
            except IndexError:
                restart_record = '1'
        else:
            restart_record = '1'
        log.debug('Restart Record is {}'.format(restart_record))

        base_url = 'https://micka.lesprojekt.cz'

        limit = self.source_config.get('datasets_per_job', 100)

        # Build template with StartPosition parameter as last parameter
        url_template = ('{base_url}/csw?' +
                        'service=CSW&version=2.0.2&request=GetRecords&TYPENAMES=record' +
                        '&sortby=title:A' +
                        '&MaxRecords={limit}' +
                        '&StartPosition={restart_record}')
        
        harvest_url = url_template.format(base_url=base_url, limit=limit, restart_record=restart_record)
        log.debug('Harvest URL is {}'.format(harvest_url))
        username = None
        password = None

        # Set the limit for the maximum number of results per job.
        # Since the new harvester jobs will be created on a rolling basis
        # via cron jobs, we don't need to grab all the results from a date
        # range at once and the harvester will resume from the last gathered
        # date each time it runs.
        
        # TO BE CONFIRMED if really needed
        timeout = self.source_config.get('timeout', 60)

        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()
         
        self.provider = 'micka_olu'   

        # This can be a hook
        
        ids = self._crawl_results(harvest_url, limit, timeout, username,
                                  password)
        # This can be a hook

        return ids

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True
