# -*- coding: utf-8 -*-

from ckan import model
from ckan.model import Package
from ckan.model import Session
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from datetime import datetime
import logging
import requests
from requests.exceptions import Timeout
import time
import uuid

from bs4 import BeautifulSoup as Soup


log = logging.getLogger(__name__)


class CSWSearchHarvester(HarvesterBase):
    """Base class for harvesters harvesting from CSW services."""

    def _get_entries_from_results(self, soup, restart_record, next_record):
        """Extract the entries from an CSWSearch response."""
        entries = []

        for entry in soup.find_all({'gmd:md_metadata'}):
            content = entry.encode()
            # The lowercase identifier will serve as the dataset's name,
            # so we need the lowercase version for the lookup in the next step.

            identifier = entry.find(['gmd:fileidentifier', 'gco:characterstring']).text.lower()   # noqa: E501
            identifier = identifier.replace('.', '_')

            guid = unicode(uuid.uuid4())

            if identifier.startswith('olu'):
                entries.append({'content': content, 'identifier': identifier,
                                'guid': guid, 'restart_record': restart_record})  # noqa: E501

        # If this job is interrupted mid-way, then the new job will re-harvest
        # the collections of this job (restart_record is the initial record)
        # If the job is finished (gone through all the entries), then the new
        # job will harvest new collections (restart_record is the next record)
        entries[-1]['restart_record'] = next_record

        return entries

    def _get_next_url(self, harvest_url, records_returned, next_record, limit):
        """
        Get the next URL.

        Return None of there is none next URL (end of results).
        """
        if next_record is not '0' and eval(records_returned) == limit:
            splitted_url = harvest_url.split('StartPosition')
            next_url = splitted_url[0] + 'StartPosition=' + next_record
            return next_url
        else:
            return None

    def _crawl_results(self, harvest_url, limit=100, timeout=5):  # noqa: E501
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
                r = requests.get(harvest_url, timeout=timeout)
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

            next_url = soup.find('csw:searchresults', elementset="summary")
            records_returned = next_url['numberofrecordsreturned']
            next_record = next_url['nextrecord']
            number_records_matched = next_url['numberofrecordsmatched']

            if next_record is not '0':
                current_record = str(eval(next_record) - eval(records_returned))  # noqa: E501
            else:
                current_record = str(eval(number_records_matched) - eval(records_returned))  # noqa: E501

            # Get the URL for the next loop, or None to break the loop
            # Only works if StartPosition is last URL parameter
            harvest_url = self._get_next_url(harvest_url, records_returned, next_record, limit)  # noqa: E501

            # Get the entries from the results
            entries = self._get_entries_from_results(soup, current_record, next_record)  # noqa: E501

            # Create a harvest object for each entry
            for entry in entries:
                entry_guid = entry['guid']
                entry_name = entry['identifier']

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

                    log.debug('{} will not be updated.'.format(entry_name))  # noqa: E501
                    status = 'unchanged'

                    obj = HarvestObject(guid=entry_guid, job=self.job,
                                        extras=[HOExtra(key='status',
                                                value=status),
                                                HOExtra(key='restart_record',
                                                value=entry['restart_record'])])  # noqa: E501
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
                                                HOExtra(key='restart_record',
                                                value=entry['restart_record'])])  # noqa: E501
                    obj.content = entry['content']
                    obj.package = None
                    obj.save()
                    ids.append(obj.id)

            end_request = time.time()
            request_time = end_request - start_request
            if request_time < 1.0:
                time.sleep(1 - request_time)

        return ids
