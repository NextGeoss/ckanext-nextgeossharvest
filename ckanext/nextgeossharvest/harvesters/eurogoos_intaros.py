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
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse

from bs4 import BeautifulSoup, SoupStrainer

from sqlalchemy import desc

from ckan import model
from ckan.model import Package

from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.eurogoos_intaros_base import EurogoosIntarosBaseHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)

class EurogoosIntarosHarvester(EurogoosIntarosBaseHarvester, NextGEOSSHarvester,
                        HarvesterBase):
    """A Harvester for EUROGOOS INTAROS Catalog."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'eurogoos_intaros',
            'title': 'EUROGOOS INTAROS Harvester',
            'description': 'A Harvester for EUROGOOS INTAROS Catalog'
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
        log = logging.getLogger(__name__ + '.gather')
        log.debug('EurogoosIntarosHarvester gather_stage for job: %r', harvest_job)

        # Save a reference
        self.job = harvest_job

        self._set_source_config(self.job.source.config)

        self.update_all = self.source_config.get('update_all', False)

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

        timeout = self.source_config.get('timeout', 60)

        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()

        if not hasattr(self, 'harvester_logger'):
            self.harvester_logger = self.make_harvester_logger()

        self.provider = 'eurogoos'

        products = self._get_products()
        # log.debug("Products: {}".format(products))

        ids = self._parse_products(products)
        log.debug("IDs: {}".format(ids))

        return ids
    
    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True

    def _get_products(self):
        """
        Create a session and return the results
        """

        req = requests.Session()
        # req.auth = (username, password)
        req.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json;charset=UTF-8',})
        
        products = []
        
        # Make a request to the website
        timestamp = str(datetime.utcnow())
        log_message = '{:<12} | {} | {} | {}s'
        try:

            url = "https://catalog-intaros.nersc.no/dataset"
            url_prefix = 'https://catalog-intaros.nersc.no'

            page = requests.get(url)    
            data = page.text
            soup = BeautifulSoup(data)

            datasets = []
            for link in soup.findAll('a'):
            # print(link.get('href'))
                if str(link.get('href')).startswith('/dataset/'):
                    dataset = url_prefix + link.get('href')
                    datasets.append(dataset)

                    html_string = dataset
                    soup = BeautifulSoup(urlopen(html_string), 'lxml') # Parse the HTML as a string

                    table_add_info = soup.findAll('table')[0]
                    table_add = str(table_add_info).replace('<th', '<td')
                    table_add = table_add.replace('\n', '')
                    table_add_list = [[cell.text for cell in row("td")]
                                            for row in BeautifulSoup(table_add)("tr")]
                    table_add_dict = dict(table_add_list)
                    table_add_dict.pop('Field')

                    resources = []
                    for item in soup.find_all(attrs={'class':'resource-list'}):
                        for link in item.find_all('a'):
                            resource=link.get('href')
                            if resource.startswith('/dataset/'):
                                resource = url_prefix + resource
                                resources.append(resource)
                    resources = list(set(resources))  #unique links only
                    # print(resources)
                    table_add_dict['uid'] = resources[0]
                    table_add_dict['Resources'] = resources
                    products.append(table_add_dict)

            return products

        except Timeout as e:
            self._save_gather_error('Request timed out: {}'.format(e), self.job)  # noqa: E501
            status_code = 408
            elapsed = 9999
            if hasattr(self, 'provider_logger'):
                self.provider_logger.info(log_message.format(self.provider,
                    timestamp, status_code, "timeout"))  # noqa: E128
            return
        if status_code != 200:
            self._save_gather_error('{} error'.format(status_code), self.job)  # noqa: E501
            elapsed = 9999
            if hasattr(self, 'provider_logger'):
                self.provider_logger.info(log_message.format(self.provider,
                    timestamp, status_code, elapsed))  # noqa: E128
            return

        if hasattr(self, 'provider_logger'):
            self.provider_logger.info(log_message.format(self.provider,
                timestamp, status_code, ''))  # noqa: E128, E501
    
    def _parse_products(self, products):  # noqa: E501
        """
        Iterate through the results, create harvest objects,
        and return the ids.
        """

        ids = []

        # Create a harvest object for each entry
        for entry in products:
            entry_guid = entry['uid']
            # entry_name = entry['filename']
            # entry_restart_date = entry['Created']
            entry_restart_date = datetime.today()
                # It's a product we haven't harvested before.
            # log.debug('{} has not been harvested before. Creating a new harvest object.'.format(entry_name))  # noqa: E501
            obj = HarvestObject(guid=entry_guid, job=self.job,
                                extras=[HOExtra(key='status',
                                        value='new'),
                                        HOExtra(key='restart_date',
                                        value=entry_restart_date)])

            obj.content = json.dumps(entry)
            obj.package = None
            obj.save()
            ids.append(obj.id)

        return ids
