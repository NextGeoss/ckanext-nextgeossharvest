# -*- coding: utf-8 -*-

import logging
import time
import json
import shapely
from datetime import datetime, date
import sys
import requests
from requests.exceptions import Timeout
from requests.auth import HTTPBasicAuth
# from urllib.request import urlopen
from pprint import pprint
import re
import socket


try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse

from bs4 import BeautifulSoup as Soup

from sqlalchemy import desc

from ckan import model
from ckan.model import Package

from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.eu_odp_base import EuOdpBaseHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)

class EuOdpHarvester(EuOdpBaseHarvester, NextGEOSSHarvester,
                        HarvesterBase):
    """A Harvester for European Union Open Data Portal Catalog."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'eu_odp',
            'title': 'EU Open Data Portal Harvester',
            'description': 'A Harvester for EU Open Data Portal Catalog'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'start_date' in config_obj:
                try:
                    datetime.strptime(config_obj['start_date'],
                                        '%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    raise ValueError('start_date format must be 2018-01-01T00:00:00Z')  # noqa: E501
            else:
                raise ValueError('start_date is required, the format must be 2018-01-01T00:00:00Z')  # noqa: E501

            if 'end_date' in config_obj:
                try:
                    datetime.strptime(config_obj['end_date'],
                                        '%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    raise ValueError('end_date format must be 2018-01-01T00:00:00Z')  # noqa: E501

            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                if not isinstance(timeout, int) and not timeout > 0:
                    raise ValueError('timeout must be a positive integer')

            if 'datasets_per_job' in config_obj:
                limit = config_obj['datasets_per_job']
                if not isinstance(limit, int) and not limit > 0:
                    raise ValueError('datasets_per_job must be a positive integer')  # noqa: E501

            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')

            if type(config_obj.get('update_all', False)) != bool:
                raise ValueError('update_all must be true or false')

        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.EuOdp.gather')
        log.debug('EUODPHarvester gather_stage for job: %r', harvest_job)

        # Save a reference
        self.job = harvest_job

        self._set_source_config(self.job.source.config)

        self.update_all = self.source_config.get('update_all', False)

        # If we need to restart, we can do so from the update time
        # of the last harvest object for the source. So, query the harvest
        # object table to get the most recently created harvest object
        # and then get its restart_date extra, and use that to restart
        # the queries, it also uses the resumption token to cycle internally
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

        start_date = self.source_config.get('start_date', '*')
        end_date = self.source_config.get('end_date', 'NOW-1DAY')

        if restart_date != '*':
            start_date = restart_date

        if start_date != '*':
            time_query = 'q=metadata_modified:[{}T00:00:00Z TO {}]'.format(start_date.split('Z')[0],
                                                                 end_date)
        else:
            time_query = ''

        limit = self.source_config.get('datasets_per_job', 100)

        base_url = 'https://data.europa.eu/euodp/data'     #ckan api

        url_template = ('{base_url}/api/3/action/package_search?' +
                        '{time_query}' +
                        '&rows={limit}')    # +
                        # '&sort=metadata_modified asc')



        harvest_url = url_template.format(base_url=base_url,
                                          time_query=time_query,
                                          limit=limit)

        log.debug('Harvest URL is {}'.format(harvest_url))

        # Set the limit for the maximum number of results per job.
        # Since the new harvester jobs will be created on a rolling basis
        # via cron jobs, we don't need to grab all the results from a date
        # range at once and the harvester will resume from the last gathered
        # date each time it runs.
        timeout = self.source_config.get('timeout', 160)

        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()

        if not hasattr(self, 'harvester_logger'):
            self.harvester_logger = self.make_harvester_logger()

        self.provider = 'euodp'

        log.debug('emtry')
        # This can be a hook
        ids = self._crawl_results(harvest_url, timeout, limit)
        # This can be a hook

        return ids

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True

    def _get_entries_from_results(self, json_result):
        """Extract the entries from an OpenSearch response."""

        entries = []

        for entry in json_result['result']['results']:
            try:
                content = entry
                identifier = entry['dataset']['uri']
                guid = entry['dataset']['uri']
                metadata_modified = entry['dataset']['modified_dcterms']['value_or_uri']
                metadata_modified.replace(" ", "T00:00:00")
                restart_date = metadata_modified
                if restart_date[-1] != 'Z':
                    restart_date = restart_date + 'Z'

                entries.append({'content': content, 'identifier': identifier,
                                    'guid': guid, 'restart_date': restart_date})
            except KeyError:
                log.debug('Not complete entry. Skipped')
                continue
            else:
                pass
        return entries

    def _get_next_url(self, harvest_url, json_result):
        """
        Get the next URL.

        Return None of there is none next URL (end of results).
        """

        if json_result['result']['count'] == 0 or json_result['result']['count'] == 1:
            return None
        else:
            last_entry = json_result['result']['results'][-1]
            restart_date = last_entry['metadata_modified']

            if restart_date[-1] != 'Z':
                restart_date = restart_date + 'Z'
            if 'q=metadata_modified' in harvest_url:
                base_url = harvest_url.split('[')[0]
                query_url = harvest_url.split('TO')[1]
                harvest_url = base_url + '[' + restart_date
                harvest_url = harvest_url + ' TO' + query_url
            else:
                time_query = 'q=metadata_modified:[{} TO NOW-1DAY]&'
                time_query = time_query.format(restart_date)
                base_url = harvest_url.split('?')[0]
                query_url = harvest_url.split('?')[1]
                harvest_url = base_url + '?' + time_query
                harvest_url = harvest_url + query_url

            return harvest_url

    def _crawl_results(self, harvest_url, timeout=5, limit=100, provider=None):  # noqa: E501
        """
        Iterate through the results, create harvest objects,
        and return the ids.
        """
        ids = []
        new_counter = 0
        first_query = True
        while len(ids) < limit and harvest_url:
            # We'll limit ourselves to one request per second
            start_request = time.time()

            # Make a request to the website
            timestamp = str(datetime.utcnow())
            log_message = '{:<12} | {} | {} | {}s'
            try:
                # headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'}
                # r = requests.get(harvest_url,
                                #  verify=False) #, timeout=timeout) #, headers=headers, family=socket.AF_INET6)
                s = requests.Session()
                r = s.get(harvest_url, verify=False) 
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
                log.debug('!= 200 ')

                if hasattr(self, 'provider_logger'):
                    self.provider_logger.info(log_message.format(self.provider,
                        timestamp, r.status_code, elapsed))  # noqa: E128
                return ids

            if hasattr(self, 'provider_logger'):
                log.debug('lastif ')

                self.provider_logger.info(log_message.format(self.provider,
                    timestamp, r.status_code, r.elapsed.total_seconds()))  # noqa: E128, E501

            content=r.content.decode('utf-8')
            # soup = Soup(r.content, 'lxml', from_encoding="utf-8")
            log.debug(harvest_url)
            # log.debug(content)
            json_content = json.loads(content)
            # json_content = json.loads(soup.text.encode('utf-8', errors="ignore"))
            # json_content = json.loads(soup.text.replace(u'\xe4', 'a').replace(u'\xe9', 'e').replace(u'\xe0', 'a').replace(u'\xdc', 'u').replace(u'\xfc', 'u').replace(u'\xdf', 'b').replace(u'\xf6', '0').decode('utf8').encode('utf8'), strict=False)
            log.debug('passedit')
            # log.debug(soup.text)
            # log.debug(json_content)
            log.debug('passedit2')



            # Get the URL for the next loop, or None to break the loop
            log.debug(harvest_url)
            # harvest_url = self._get_next_url(harvest_url, json_content)
            harvest_url = None

            # Get the entries from the results
            entry_list = self._get_entries_from_results(json_content)

            # if first_query:
            #     entries = entry_list
            # else:
            #     entries = entry_list[1:]

            # first_query = False

            # Create a harvest object for each entry
            for entry in entry_list:
                entry_guid = entry['guid']
                entry_name = entry['identifier']
                entry_restart_date = entry['restart_date']
                log.debug('emtry : {}'.format(entry))

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

                    if self.update_all:
                        log.debug('{} already exists and will be updated.'.format(entry_name))  # noqa: E501
                        status = 'change'
                    else:
                        log.debug('{} will not be updated.'.format(entry_name))  # noqa: E501
                        status = 'unchanged'

                    obj = HarvestObject(guid=entry_guid, job=self.job,
                                        extras=[HOExtra(key='status',
                                                value=status),
                                                HOExtra(key='restart_date',
                                                value=entry_restart_date)])
                    obj.content = json.dumps(entry['content'])
                    obj.package = package
                    obj.save()
                    ids.append(obj.id)

                elif not package:
                    # It's a product we haven't harvested before.
                    log.debug('{} has not been harvested before. Creating a new harvest object.'.format(entry_name))  # noqa: E501
                    obj = HarvestObject(guid=entry_guid, job=self.job,
                                        extras=[HOExtra(key='status',
                                                value='new'),
                                                HOExtra(key='restart_date',
                                                value=entry_restart_date)])
                    new_counter += 1
                    obj.content = json.dumps(entry['content'])
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
