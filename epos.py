# -*- coding: utf-8 -*-

import logging
import time
import json
from datetime import datetime

import requests
from requests.exceptions import Timeout

from bs4 import BeautifulSoup as Soup

import uuid

from sqlalchemy import desc

from ckan import model
from ckan.model import Package

from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.epos_base import EPOSbaseHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class EPOSHarvester(EPOSbaseHarvester, NextGEOSSHarvester, HarvesterBase):
    """A Harvester for EPOS Sat Products."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'epossat',
            'title': 'EPOS Sat Harvester',
            'description': 'A Harvester for EPOS Sat Products'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if config_obj.get('collection') not in {'inu', 'inw', 'dts', 'coh', 'aps', 'cosneu'}:  # noqa: E501
                raise ValueError('collection is required and must be either inu, inw, dts, coh, aps, cosneu')  # noqa: E501
                # add missing collections
            if 'start_date' in config_obj:
                try:
                    datetime.strptime(config_obj['start_date'],
                                      '%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    raise ValueError('start_date format must be 2018-01-01T00:00:00Z')  # noqa: E501
            else:
                raise ValueError('start_date is required and the format must be 2018-01-01T00:00:00Z')  # noqa: E501
            if 'end_date' in config_obj:
                try:
                    datetime.strptime(config_obj['end_date'],
                                      '%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    raise ValueError('end_date format must be 2018-01-01T00:00:00Z')  # noqa: E501

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

        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.EPOSSat.gather')
        log.debug('EPOSSatHarvester gather_stage for job: %r', harvest_job)

        # Save a reference
        self.job = harvest_job

        self._set_source_config(self.job.source.config)

        self.update_all = self.source_config.get('update_all', False)

        # If we need to restart, we can do so from the ingestion timestamp
        # of the last harvest object for the source. So, query the harvest
        # object table to get the most recently created harvest object
        # and then get its restart_page extra, and use that to restart
        # the queries
        last_object = Session.query(HarvestObject). \
            filter(HarvestObject.harvest_source_id == self.job.source_id,
                   HarvestObject.import_finished != None). \
            order_by(desc(HarvestObject.import_finished)).limit(1)  # noqa: E711, E501
        if last_object:
            try:
                last_object = last_object[0]
                restart_page = self._get_object_extra(last_object,
                                                      'restart_page', '1')
            except IndexError:
                restart_page = '1'
        else:
            restart_page = '1'
        log.debug('Restart page is {}'.format(restart_page))

        start_date = self.source_config.get('start_date', restart_page)
        start_date_url = 'start={}'.format(start_date)
        end_date = self.source_config.get('end_date', 'NOW')
        if end_date == 'NOW':
            end_date_url = ''
        else:
            end_date_url = 'end={}'.format(end_date)

        # Get the base_url
        source = self.source_config.get('collection')
        base_url = 'https://catalog.terradue.com'

        if source == 'inu':
            collection = 'pt=UNWRAPPED_INTERFEROGRAM'
        elif source == 'inw':
            collection = 'pt=WRAPPED_INTERFEROGRAM'
        elif source == 'dts':
            collection = 'pt=LOS_DISPLACEMENT_TIMESERIES'
        elif source == 'coh':
            collection = 'pt=SPATIAL_COHERENCE'
        elif source == 'aps':
            collection = 'pt=INTERFEROGRAM_APS_GLOBAL_MODEL'
        elif source == 'cosneu':
            collection = 'pt=MAP_OF_LOS_VECTOR'

        url_template = ('{base_url}/gep-epos/search?' +
                        '{start_date}' +
                        '&{end_date}' +
                        '&{collection}' +
                        '&startIndex={restart_page}')
        harvest_url = url_template.format(base_url=base_url,
                                          start_date=start_date_url,
                                          end_date=end_date_url,
                                          collection=collection,
                                          restart_page=restart_page)

        log.debug('Harvest URL is {}'.format(harvest_url))

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

        self.provider = 'epos'

        limit = self.source_config.get('datasets_per_job', 100)

        # This can be a hook
        ids = self._crawl_results(harvest_url, timeout, limit)
        # This can be a hook

        return ids

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True

    def _get_entries_from_results(self, soup):
        """Extract the entries from an OpenSearch response."""
        entries = []
        restart_page = soup.find('startindex').text
        for entry in soup.find_all('entry'):
            content = entry.encode()
            # The lowercase identifier will serve as the dataset's name,
            # so we need the lowercase version for the lookup in the next step.
            identifier = entry.find('identifier').text.lower()  # noqa: E501
            identifier = identifier.replace('-', '_')
            guid = unicode(uuid.uuid4())

            entries.append({'content': content, 'identifier': identifier,
                            'guid': guid, 'restart_page': restart_page})

        return entries

    def _get_next_url(self, harvest_url, soup):
        """
        Get the next URL.

        Return None of there is none next URL (end of results).
        """

        total_results = eval(soup.find('totalresults').text)
        items_per_page = eval(soup.find('itemsperpage').text)
        start_page = eval(soup.find('startindex').text)

        records_ratio = float(total_results) / (start_page * items_per_page)
        if records_ratio > 1:
            splitted_url = harvest_url.split('StartPage')
            next_url = splitted_url[0] + 'StartPage=' + str(start_page + 1)
            return next_url
        else:
            return None

    def _crawl_results(self, harvest_url, timeout=5, limit=100, provider=None):  # noqa: E501
        """
        Iterate through the results, create harvest objects,
        and return the ids.
        """
        ids = []
        new_counter = 0

        while len(ids) < limit and harvest_url:
            # We'll limit ourselves to one request per second
            start_request = time.time()

            # Make a request to the website
            timestamp = str(datetime.utcnow())
            log_message = '{:<12} | {} | {} | {}s'
            try:
                r = requests.get(harvest_url,
                                 verify=False, timeout=timeout)
            except Timeout as e:
                self._save_gather_error('Request timed out: {}'.format(e), self.job)  # noqa: E501
                status_code = 408
                elapsed = 9999
                if hasattr(self, 'provider_logger'):
                    self.provider_logger.info(log_message.format(self.provider,
                        timestamp, status_code, timeout))  # noqa: E128
                return ids
            if r.status_code != 200:
                self._save_gather_error('{} error: {}'.format(r.status_code, r.text), self.job)  # noqa: E501
                elapsed = 9999
                if hasattr(self, 'provider_logger'):
                    self.provider_logger.info(log_message.format(self.provider,
                        timestamp, r.status_code, elapsed))  # noqa: E128
                return ids

            if hasattr(self, 'provider_logger'):
                self.provider_logger.info(log_message.format(self.provider,
                    timestamp, r.status_code, r.elapsed.total_seconds()))  # noqa: E128, E501

            soup = Soup(r.content, 'lxml')

            # Get the URL for the next loop, or None to break the loop
            harvest_url = self._get_next_url(harvest_url, soup)

            # Get the entries from the results
            entries = self._get_entries_from_results(soup)

            # Create a harvest object for each entry
            for entry in entries:
                entry_guid = entry['guid']
                entry_name = entry['identifier']
                entry_restart_page = entry['restart_page']

                package = Session.query(Package) \
                    .filter(Package.name == entry_name).first()

                if package:
                    # Meaning we've previously harvested this,
                    # but we may want to reharvest it now.
                    previous_obj = model.Session.query(HarvestObject) \
                        .filter(HarvestObject.guid == entry_guid) \
                        .filter(HarvestObject.current == True) \
                        .first()  # noqa: E712
                    if previous_obj:
                        previous_obj.current = False
                        previous_obj.save()

                    # If the package already exists it
                    # will not create a new one
                    log.debug('{} will not be updated.'.format(entry_name))  # noqa: E501
                    status = 'unchanged'

                    obj = HarvestObject(guid=entry_guid, job=self.job,
                                        extras=[HOExtra(key='status',
                                                value=status),
                                                HOExtra(key='restart_page',
                                                value=entry_restart_page)])
                    obj.content = entry['content']
                    obj.package = package
                    obj.save()

                elif not package:
                    # It's a product we haven't harvested before.
                    log.debug('{} has not been harvested before. Creating a new harvest object.'.format(entry_name))  # noqa: E501
                    obj = HarvestObject(guid=entry_guid, job=self.job,
                                        extras=[HOExtra(key='status',
                                                value='new'),
                                                HOExtra(key='restart_page',
                                                value=entry_restart_page)])
                    new_counter += 1
                    obj.content = entry['content']
                    obj.package = None
                    obj.save()
                    ids.append(obj.id)

            end_request = time.time()
            request_time = end_request - start_request
            if request_time < 1.0:
                time.sleep(1 - request_time)

        harvester_msg = '{:<12} | {} | jobID:{} | {} | {}'
        if hasattr(self, 'harvester_logger'):
            timestamp = str(datetime.utcnow())
            self.harvester_logger.info(harvester_msg.format(self.provider,
                                       timestamp, self.job.id, new_counter, 0))  # noqa: E128, E501

        return ids
