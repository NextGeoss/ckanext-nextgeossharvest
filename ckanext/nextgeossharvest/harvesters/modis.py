# -*- coding: utf-8 -*-

import logging
import json
from datetime import datetime
from datetime import timedelta

from sqlalchemy import desc

from ckan.common import config
from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.modis_base import CMRHarvester
from ckanext.nextgeossharvest.lib.opensearch_base import OpenSearchHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester


class MODISHarvester(CMRHarvester, OpenSearchHarvester, NextGEOSSHarvester):
    """A Harvester for MODIS Products."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'modis',
            'title': 'MODIS Harvester',
            'description': 'A Harvester for MODIS Products'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            # Dictionary with allowed collection,
            # and the minimum start date
            MODIScatalogue = {}

            MODIScatalogue['MYD13Q1'] = '2002-07-01T00:00:00Z'
            MODIScatalogue['MYD13A1'] = '2002-07-01T00:00:00Z'
            MODIScatalogue['MYD13A2'] = '2002-07-01T00:00:00Z'
            MODIScatalogue['MOD13Q1'] = '2000-02-01T00:00:00Z'
            MODIScatalogue['MOD13A1'] = '2000-02-01T00:00:00Z'
            MODIScatalogue['MOD13A2'] = '2000-02-01T00:00:00Z'
            MODIScatalogue['MOD17A3H'] = '2002-01-01T00:00:00Z'
            MODIScatalogue['MOD17A2H'] = '2000-02-01T00:00:00Z'
            MODIScatalogue['MYD15A2H'] = '2002-07-01T00:00:00Z'
            MODIScatalogue['MOD15A2H'] = '2002-07-01T00:00:00Z'
            MODIScatalogue['MOD14A2'] = '2000-02-01T00:00:00Z'
            MODIScatalogue['MYD14A2'] = '2002-07-01T00:00:00Z'

            if config_obj.get('collection') not in MODIScatalogue:  # noqa: E501
                raise ValueError('collection is required and must be one of the entries of [MYD13Q1, MYD13A1, MYD13A2, MOD13Q1, MOD13A1, MOD13A2, MOD17A3H, MOD17A2H, MYD15A2H, MOD15A2H, MOD14A2, MYD14A2]')  # noqa: E501
            if 'start_date' in config_obj:
                try:
                    datetime.strptime(config_obj['start_date'],
                                      '%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    raise ValueError('start_date format must be 2018-01-01T00:00:00Z')  # noqa: E501
            if 'datasets_per_job' in config_obj:
                limit = config_obj['datasets_per_job']
                if not isinstance(limit, int) and not limit > 0:
                    raise ValueError('datasets_per_job must be a positive integer')  # noqa: E501
            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                if not isinstance(timeout, int) and not timeout > 0:
                    raise ValueError('timeout must be a positive integer')
            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')
            if type(config_obj.get('update_all', False)) != bool:
                raise ValueError('update_all must be true or false')

        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.MODIS.gather')
        log.debug('MODIS gather_stage for job: %r', harvest_job)

        # Save a reference
        self.job = harvest_job
        self._set_source_config(self.job.source.config)

        collection = self.source_config.get('collection')
        limit = self.source_config.get('datasets_per_job', 2000)

        # Necessary due to the fact that opensearch_base.py requiring it
        # Although this value can be always set to False, since for the
        # same product there is always just one provider
        self.update_all = self.source_config.get('update_all', False)

        # If we need to restart, we can do so from the start_date timestamp
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

        if '*' in restart_date:
            start_date = self.source_config.get('start_date', None)
        else:
            lastdataset_enddate = restart_date.split('/')[1]
            enddate_datetime = datetime.strptime(lastdataset_enddate, '%Y-%m-%dT%H:%M:%S.000Z')  # noqa: E501

            if collection in ['MOD17A3H']:
                # This collection has yearly datasets, however the time range
                # of the datasets is from the last week of December until the
                # the first week of January of the next year
                # Thus, this collection requires a different processing, where
                # the start date is always YYYY-01-01T00:00:00Z, and the
                # end date is (YYYY+1)-12-31T23:59:59Z
                start_date = enddate_datetime.strftime('%Y-01-01T00:00:00Z')

            else:
                # This is for the scenarios where the next dataset START date
                # is 1 SECOND after the END date of the last dataset
                restart_date = enddate_datetime + timedelta(seconds=1)
                start_date = restart_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        log.debug('Restart date is {}'.format(restart_date))

        if collection in ['MOD17A2H', 'MYD15A2H', 'MOD15A2H', 'MOD14A2', 'MYD14A2']:  # noqa: E501
            # The time resolution of these datasets is 8 days, thus deltaT:
            # 7days + 23hours + 59minutes + 59seconds
            deltaT = 7 * 24 * 3600 + 23 * 3600 + 59 * 60 + 59  # noqa: E501
        elif collection in ['MYD13Q1', 'MYD13A1', 'MYD13A2', 'MOD13Q1', 'MOD13A1', 'MOD13A2']:  # noqa: E501
            # The time resolution of these datasets is 16 days, thus deltaT:
            # 15days + 23hours + 59minutes + 59seconds
            deltaT = 15 * 24 * 3600 + 23 * 3600 + 59 * 60 + 59  # noqa: E501

        if collection in ['MOD17A3H']:
            # Since the time resolution of this collection is 1 year,
            # the time interval on the query will be from 01-01-xxxx
            # to 31-12-xxxx
            end_date_partial = '-12-31T23:59:59Z'
            final_date = start_date.split('-')[0] + end_date_partial
        else:
            startdate_dateformat = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%SZ')  # noqa: E501
            enddate_dateformat = startdate_dateformat + timedelta(seconds=deltaT)  # noqa: E501
            final_date = enddate_dateformat.strftime('%Y-%m-%dT%H:%M:%SZ')

        end_date = self.source_config.get('end_date', final_date)  # noqa: E501

        if start_date:
            date_range = '&startTime={}&endTime={}'.format(start_date, end_date)  # noqa: E501
        else:
            date_range = ''

        self.os_id_name = 'dc:identifier'
        self.os_id_attr = None
        self.os_guid_name = 'dc:identifier'
        self.os_guid_attr = None
        self.os_restart_date_name = 'dc:date'
        self.os_restart_date_attr = None
        self.os_restart_filter = None
        self.flagged_extra = None

        source_url = 'https://cmr.earthdata.nasa.gov/opensearch/granules.atom?'
        base_url = source_url + 'clientId=cswOpenSearchDoc'

        url_template = ('{base_url}' +
                        '&shortName={collection}' +
                        '&numberOfResults={limit}' +
                        '{date_range}')

        harvest_url = url_template.format(base_url=base_url,
                                          collection=collection,
                                          limit=limit,
                                          date_range=date_range)

        log.debug('Harvest URL is {}'.format(harvest_url))
        username = config.get('ckanext.nextgeossharvest.nextgeoss_username')
        password = config.get('ckanext.nextgeossharvest.nextgeoss_password')

        # Set the limit for the maximum number of results per job.
        # Since the new harvester jobs will be created on a rolling basis
        # via cron jobs, we don't need to grab all the results from a date
        # range at once and the harvester will resume from the last gathered
        # date each time it runs.
        timeout = self.source_config.get('timeout', 10)

        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()

        if not hasattr(self, 'harvester_logger'):
            self.harvester_logger = self.make_harvester_logger()

        self.provider = 'nasa_cmr'
        # This can be a hook
        ids = self._crawl_results(harvest_url, limit, timeout, username,
                                  password)

        '''
        Note #1:
        Day 2019-01-16
        Currently, the harvester is assuming that there are datasets every time
        interval, and thus starts the next query at the end date of the last
        dataset harvested plus 1 second.

        Problem: If there is a time interval without any dataset (although not
        not seen until now, it can happen), the harvester will get "stuck" on
        that time interval without retrieving new data

        Immediate Solution: Manually restart the harvester at a timeinterval
        with datasets

        Robust Solution: Check the number of ids retrieved from the gather
        stage, if it is 0, then instead of querying for start_date + deltaT,
        query for the time interval start_date until datetime.NOW, from the
        1st id retrieve the start_date, and resume with the query
        start_date + deltaT, the pseudo-code is as follows:

        if len(ids) == 0:
            new_url = harvest_url(from start_date until NOW)
            new_ids = self.craw_results(new_url)
            if new_ids != 0:
                new_start = ids[0].start_date
                hav_url = harvest_url(from new_start until new_start+deltaT)
                ids = self.crawl_results(hav_url)

        Current Impediment: Retrieve start_date from ids[0]
        ToDo: Check how to access package from id in this function
        '''
        # This can be a hook
        if len(ids) == 0:
            date_now = datetime.now()
            date_now_str = date_now.strftime('%Y-%m-%dT%H:%M:%SZ')
            if start_date:
                date_range = '&startTime={}&endTime={}'.format(start_date, date_now_str)  # noqa: E501
            else:
                date_range = ''
            harvest_url = url_template.format(base_url=base_url,
                                              collection=collection,
                                              limit=limit,
                                              date_range=date_range)
            log.debug('Harvest URL is {}'.format(harvest_url))
            ids = self._crawl_results(harvest_url, limit, timeout, username,
                                      password)

            if len(ids) == 0:
                current_start = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%SZ')  # noqa: E501
                previous_end = current_start - timedelta(seconds=1)
                previous_end_str = previous_end.strftime('%Y-%m-%dT%H:%M:%SZ')
                previous_start = previous_end - timedelta(seconds=deltaT)
                previous_start_str = previous_start.strftime('%Y-%m-%dT%H:%M:%SZ')  # noqa: E501
                date_range = '&startTime={}&endTime={}'.format(previous_start_str, previous_end_str)  # noqa: E501
                harvest_url = url_template.format(base_url=base_url,
                                                  collection=collection,
                                                  limit=limit,
                                                  date_range=date_range)
                log.debug('Harvest URL is {}'.format(harvest_url))
                ids = self._crawl_results(harvest_url, limit, timeout, username, password)  # noqa: E501
            elif len(ids) == limit:
                # Develop Note #1 (TBD, or TBC if really needed)
                ids = []

        return ids

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True
