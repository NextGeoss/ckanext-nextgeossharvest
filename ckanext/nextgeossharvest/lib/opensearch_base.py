# -*- coding: utf-8 -*-

import logging
import time
from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import Timeout
from bs4 import BeautifulSoup as Soup

from ckan import model
import ckan.logic as logic
from ckan.lib.helpers import get_pkg_dict_extra
from ckan.model import Session
from ckan.model import Package

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase


log = logging.getLogger(__name__)


class OpenSearchHarvester(HarvesterBase):
    """Base class for harvesters harvesting from OpenSearch services."""

    def _get_entries_from_results(self, soup):
        """Extract the entries from an OpenSearch response."""
        entries = []

        for entry in soup.find_all('entry'):
            content = entry.encode()
            # The lowercase identifier will serve as the dataset's name,
            # so we need the lowercase version for the lookup in the next step.
            identifier = entry.find(self.os_id_name, self.os_id_attr).text.lower()  # noqa: E501
            if hasattr(self, 'os_id_mod'):
                identifier = self.os_id_mod(identifier)
            guid = entry.find(self.os_guid_name, self.os_guid_attr).text
            if hasattr(self, 'os_guid_mod'):
                guid = self.os_guid_mod(guid)
            restart_date = entry.find(self.os_restart_date_name, self.os_restart_date_attr).text  # noqa: E501
            if hasattr(self, 'os_restart_date_mod'):
                restart_date = self.os_restart_date_mod(restart_date)
            entries.append({'content': content, 'identifier': identifier,
                            'guid': guid, 'restart_date': restart_date})

        return entries

    def _get_next_url(self, soup):
        """
        Get the next URL.

        Return None of there is none next URL (end of results).
        """
        next_url = soup.find('link', rel='next')
        if next_url:
            return next_url['href']
        else:
            return None

    def _crawl_results(self, harvest_url, limit=100, timeout=5, username=None, password=None, provider=None):  # noqa: E501
        """
        Iterate through the results, create harvest objects,
        and return the ids.
        """
        ids = []

        while len(ids) < limit and harvest_url:
            # We'll limit ourselves to one request per second
            start_request = time.time()

            # Make a request to the website
            timestamp = str(datetime.utcnow())
            log_message = '{:<12} | {} | {} | {}s'
            try:
                r = requests.get(harvest_url,
                                 auth=HTTPBasicAuth(username, password),
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
            harvest_url = self._get_next_url(soup)

            # Get the entries from the results
            entries = self._get_entries_from_results(soup)

            # Create a harvest object for each entry
            for entry in entries:
                entry_guid = entry['guid']
                entry_name = entry['identifier']
                entry_restart_date = entry['restart_date']

                package = Session.query(Package) \
                    .filter(Package.name == entry_name).first()

                if package:
                    # Meaning we've previously harvested this,
                    # but we may want to reharvest it now.
                    # We need package_show to ensure that all the conversions
                    # are carried out.
                    context = {"user": "test_user", "ignore_auth": True,
                               "model": model, "session": Session}
                    pkg_dict = logic.get_action('package_show')(context,{"id": package.name})  # noqa: E501
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
                    # E.g., a Sentinel dataset exists,
                    # but doesn't have a NOA resource yet.
                    elif self.flagged_extra and not get_pkg_dict_extra(pkg_dict, self.flagged_extra):  # noqa: E501
                        log.debug('{} already exists and will be extended.'.format(entry_name))  # noqa: E501
                        status = 'change'
                    else:
                        log.debug('{} will not be updated.'.format(entry_name))  # noqa: E501
                        status = 'unchanged'

                    obj = HarvestObject(guid=entry_guid, job=self.job,
                                        extras=[HOExtra(key='status',
                                                value=status),
                                                HOExtra(key='restart_date',
                                                value=entry_restart_date)])
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
                                                value=entry_restart_date)])
                    obj.content = entry['content']
                    obj.package = None
                    obj.save()
                    ids.append(obj.id)

            end_request = time.time()
            request_time = end_request - start_request
            if request_time < 1.0:
                time.sleep(1 - request_time)

        return ids
