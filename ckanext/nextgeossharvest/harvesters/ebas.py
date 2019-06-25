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

from ckanext.nextgeossharvest.lib.ebas_base import EBASbaseHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase
from ckan.controllers import package

log = logging.getLogger(__name__)


class EBASHarvester(EBASbaseHarvester, NextGEOSSHarvester, HarvesterBase):
    """A Harvester for EBAS Products."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'ebas',
            'title': 'EBAS Harvester',
            'description': 'A Harvester for EBAS Products'
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

            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')

        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.EBAS.gather')
        log.debug('EBASHarvester gather_stage for job: %r', harvest_job)

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
                restart_token = self._get_object_extra(last_object,
                                                       'restart_token', None)
            except IndexError:
                restart_date = '*'
                restart_token = None
        else:
            restart_date = '*'
            restart_token = None

        log.debug('Restart date is {}'.format(restart_date))
        log.debug('Restart token is {}'.format(restart_token))

        start_date = self.source_config.get('start_date', '*')
        end_date = self.source_config.get('end_date', '*')

        if restart_date != '*' and end_date == '*':
            start_date_url = '&from={}'.format(restart_date)
        elif start_date != '*':
            start_date_url = '&from={}'.format(start_date)
        else:
            start_date_url = ''

        if end_date == '*':
            end_date_url = ''
        else:
            end_date_url = '&until={}'.format(end_date)

        md_prefix = self.source_config.get('metadata_prefix', 'iso19115')
        set_db = self.source_config.get('set', 'ebas-db')

        md_prefix_url = '&metadataPrefix={}'.format(md_prefix)
        set_url = '&set={}'.format(set_db)

        base_url = 'https://ebas-oai-pmh.nilu.no'

        if restart_token:
            token = '&resumptionToken={}'.format(restart_token)

            url_template = ('{base_url}/oai/provider?' +
                            'verb=ListRecords' +
                            '{resumptionToken}')

            harvest_url = url_template.format(base_url=base_url,
                                              resumptionToken=token)

        else:
            url_template = ('{base_url}/oai/provider?' +
                            'verb=ListRecords' +
                            '{md_prefix}' +
                            '{set_db}' +
                            '{start_date}' +
                            '{end_date}')

            harvest_url = url_template.format(base_url=base_url,
                                              md_prefix=md_prefix_url,
                                              set_db=set_url,
                                              start_date=start_date_url,
                                              end_date=end_date_url)

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

        self.provider = 'ebas'

        limit = self.source_config.get('datasets_per_job', 500)

        # This can be a hook
        ids = self._crawl_results(harvest_url, restart_date,
                                  restart_token, timeout, limit)
        # This can be a hook

        return ids

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True

    def is_deleted(self, header):
        """
        Returns the state of the dataset.

        Return False if the dataset is valid, True otherwise.
        """

        try:
            status = header['status']
            if status == 'deleted':
                return True
            else:
                return False
        except:
            return False

    def _get_entries_from_results(self, soup, restart_date, token):
        """Extract the entries from an OpenSearch response."""
        entries = []

        for entry in soup.find_all('record'):
            header = entry.find('header')

            if not self.is_deleted(header):
                content = entry.encode()

                datestamp = entry.find('datestamp').text
                if restart_date == '*' or restart_date > datestamp:
                    restart_date = datestamp

                # The lowercase identifier will serve as the dataset's name,
                # so we need the lowercase version for the lookup in the next
                # step.
                identifier = entry.find('identifier').text.strip('\n')

                guid = unicode(uuid.uuid4())

                entries.append({'content': content, 'identifier': identifier,
                                'guid': guid, 'restart_date': restart_date,
                                'restart_token': token})

        token = soup.find('resumptiontoken').text

        if len(entries) > 0:
            entries[-1]['restart_token'] = token

        return entries

    def _get_next_url(self, harvest_url, soup):
        """
        Get the next URL.

        Return None of there is none next URL (end of results).
        """

        base_url = harvest_url.split('&')[0]
        token = soup.find('resumptiontoken').text

        if token:
            tmp_url = base_url + '&resumptionToken={}'
            next_url = tmp_url.format(token)
            return next_url
        else:
            return None

    def _search_package(self, identifier):

        name = identifier.lower()
        replace_chars = [',', ':', '.', '/', '-']

        for x in replace_chars:
            name = name.replace(x, '_')

        name = name.replace('oai_ebas_oai_pmh_nilu_no_', '')
        template_name = name[0:42]

        MAX_NUMBER_APPENDED = 999999
        PACKAGE_NAME_MAX_LENGTH = 99
        APPEND_MAX_CHARS = len(str(MAX_NUMBER_APPENDED))

        # Find out which package names have been taken. Restrict it to names
        # derived from the ideal name plus and numbers added
        like_q = u'%s%%' % \
            template_name[:PACKAGE_NAME_MAX_LENGTH-APPEND_MAX_CHARS]
        results = Session.query(Package)\
                         .filter(Package.name.ilike(like_q))\
                         .all()
        if results:
            for result in results:
                package_dict = self._get_package_dict(result)
                extra_identifier = self._get_package_extra(package_dict,
                                                           'identifier')

                if identifier == extra_identifier:
                    return result
                else:
                    return None
        else:
            return None

    def _crawl_results(self, harvest_url, restart_date, token, timeout=5, limit=100, provider=None):  # noqa: E501
        """
        Iterate through the results, create harvest objects,
        and return the ids.
        """
        ids = []
        new_counter = 0
        update_counter = 0
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
            entries = self._get_entries_from_results(soup,
                                                     restart_date, token)

            # Create a harvest object for each entry
            for entry in entries:
                entry_guid = entry['guid']
                entry_name = entry['identifier']
                entry_restart_date = entry['restart_date']
                entry_restart_token = entry['restart_token']

                package = self._search_package(entry_name)

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
                    log.debug('{} already exists and will be updated.'.format(entry_name))  # noqa: E501
                    status = 'change'

                    obj = HarvestObject(guid=entry_guid, job=self.job,
                                        extras=[HOExtra(key='status',
                                                value=status),
                                                HOExtra(key='restart_date',
                                                value=entry_restart_date),
                                                HOExtra(key='restart_token',
                                                value=entry_restart_token)])
                    update_counter += 1
                    obj.content = entry['content']
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
                                                value=entry_restart_date),
                                                HOExtra(key='restart_token',
                                                value=entry_restart_token)])
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
                                       timestamp, self.job.id, new_counter, update_counter))  # noqa: E128, E501

        return ids
