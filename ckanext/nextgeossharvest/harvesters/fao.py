# -*- coding: utf-8 -*-

import logging
import time
import json
import xmltodict
import re
import stringcase
from datetime import datetime

import requests
from requests.exceptions import Timeout
from sqlalchemy import desc

from ckan import model
from ckan.model import Package

from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.fao_base import FaoBaseHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class FaoHarvester(FaoBaseHarvester, NextGEOSSHarvester,
                        HarvesterBase):
    """A Harvester for Fao Products."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'FAO',
            'title': 'FAO Harvester',
            'description': 'A Harvester for FAO Products'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'start_date' in config_obj:
                try:
                    datetime.strptime(config_obj['start_date'],
                                      '%Y-%m-%dT%H:%M:%S')
                except ValueError:
                    raise ValueError('start_date format must be 2018-01-01T00:00:00')  # noqa: E501
            else:
                raise ValueError('start_date is required, the format must be 2018-01-01T00:00:00')  # noqa: E501

            if 'end_date' in config_obj:
                try:
                    datetime.strptime(config_obj['end_date'],
                                      '%Y-%m-%dT%H:%M:%S')
                except ValueError:
                    raise ValueError('end_date format must be 2018-01-01T00:00:00')  # noqa: E501

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
        log = logging.getLogger(__name__ + '.Fao.gather')
        log.debug('FaoHarvester gather_stage for job: %r', harvest_job)

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
                restart_date = self._get_object_extra(last_object, 'restart_date', '*')
            except IndexError:
                restart_date = ''
        else:
            restart_date = ''

        log.debug('Restart date is {}'.format(restart_date))

        start_date = self.source_config.get('start_date', '')
        end_date = self.source_config.get('end_date', '')

        if restart_date != '':
            start_date = restart_date

        if start_date != '':
            time_query = 'dateFrom={}&dateTo={}'.format(start_date, end_date)
        else:
            time_query = ''

        limit = self.source_config.get('datasets_per_job', 100)

        # Old URL
        # base_url = 'http://www.fao.org'

        # url_template = ('{base_url}/geonetwork/srv/en/xml.search?' +
        #                 '{time_query}' +
        #                 '&from=1&to={limit}' +
        #                 '&sortBy=changeDate&sortOrder=reverse&fast=false')

        base_url = 'https://data.apps.fao.org'

        url_template = ('{base_url}/map/catalog/srv/eng/xml.search?' +
                        '{time_query}' +
                        '&from=1&to={limit}' +
                        '&sortBy=changeDate&sortOrder=reverse&fast=false')

        harvest_url = url_template.format(base_url=base_url,
                                          time_query=time_query,
                                          limit=limit)

        log.debug('Harvest URL is {}'.format(harvest_url))

        # Set the limit for the maximum number of results per job.
        # Since the new harvester jobs will be created on a rolling basis
        # via cron jobs, we don't need to grab all the results from a date
        # range at once and the harvester will resume from the last gathered
        # date each time it runs.
        timeout = self.source_config.get('timeout', 60)

        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()

        if not hasattr(self, 'harvester_logger'):
            self.harvester_logger = self.make_harvester_logger()

        self.provider = 'Fao'

        ids = []
        limit_to = 1
        limit = int(limit)
        # This can be a hook
        if limit <= 100:
            ids = self._crawl_results(harvest_url, timeout, limit)
        else:
            while limit-limit_to > 0:
                url_template = ('{base_url}/map/catalog/srv/eng/xml.search?' +
                    '{time_query}' +
                    '&from={limit_to}&to={limit}' +
                    '&sortBy=changeDate&sortOrder=reverse&fast=false')

                harvest_url = url_template.format(base_url=base_url,
                                                time_query=time_query,
                                                limit_to=limit_to,
                                                limit=limit)

                try:
                    results = self._crawl_results(harvest_url, timeout, limit)
                    ids.extend(results)
                # No products found
                except KeyError:
                    break

                limit_to += 100
        # This can be a hook

        # Return products
        return [item for item in ids]

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True

    def _get_entries_from_results(self, results):
        """Extract the entries from a Geonetwork response."""

        entries = []

        for entry in results:
            # Create identifier from Title and ID if they exist
            try:
                identifier = self._create_identifier(entry['title'], entry['geonet:info']['id']['#text'])
            except TypeError:
                continue

            # Skip datasets with short vague names
            # Example dataset name: i_29_12_04
            if len(identifier) < 16:
                continue

            guid = entry['geonet:info']['uuid']['#text'].encode('utf-8')
            date = entry['geonet:info']['changeDate']['#text'].encode('utf-8')
            
            # Check if dataset contains any resources. Skip it if it doesn't
            if 'link' in entry:
                # Manually parse useful XML fields and convert to dictionary
                content_dict = {
                    'title' : identifier,
                    'date' : date,
                    'links' : entry['link']
                }
                if 'abstract' in entry:
                    content_dict['description'] = entry['abstract']
                else:
                    content_dict['description'] = ""
                if 'tags' in entry:
                    content_dict['tags'] = entry['keyword']
                else:
                    content_dict['tags'] = []
                if 'image' in entry:
                    content_dict['images'] = entry['image']
                else:
                    content_dict['images'] = []
                if 'geoBox' in entry:
                    content_dict['geometry'] = entry['geoBox']
                else:
                    content_dict['geometry'] = {'westBL': '0', 'eastBL': '0', 'southBL': '0', 'northBL': '0',}
            else:
                continue

            entries.append({'content': content_dict, 'identifier': identifier,
                        'guid': guid, 'restart_date': date})

        return entries

    def _crawl_results(self, harvest_url, timeout=10, limit=100, provider=None):  # noqa: E501
        """
        Iterate through the results, create harvest objects,
        and return the ids.
        """
        ids = []
        new_counter = 0
        
        # Make a request to the website
        timestamp = str(datetime.utcnow())
        log_message = '{:<12} | {} | {} | {}s'
        try:
            r = requests.get(harvest_url, verify=False, timeout=timeout)
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

        # Get results
        # Convert to Dictionaries from XML
        dict_content = xmltodict.parse(r.content)['response']['metadata']

        # Get the entries from the results
        entry_list = self._get_entries_from_results(dict_content)

        # Create a harvest object for each entry
        for entry in entry_list:
            entry_guid = entry['guid']
            entry_name = entry['identifier']
            entry_restart_date = entry['restart_date']

            package = Session.query(Package).filter(Package.name == entry_name).first()

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

        harvester_msg = '{:<12} | {} | jobID:{} | {} | {}'
        if hasattr(self, 'harvester_logger'):
            timestamp = str(datetime.utcnow())
            self.harvester_logger.info(harvester_msg.format(self.provider,
                                        timestamp, self.job.id, new_counter, 0))  # noqa: E128, E501

        return ids

    def _create_identifier(self, og_identifier, og_id):
        """ Creates identifier from title and ID.
            Keeps only alphanumeric characters.
        """

        identifier = og_identifier + "_" + og_id

        identifier = re.sub('[^0-9a-zA-Z]+', '_', identifier).lower()
        sc_identifier = stringcase.snakecase(identifier).strip("_")
        while "__" in sc_identifier:
            sc_identifier = sc_identifier.replace("__", "_")
        return sc_identifier
