# -*- coding: utf-8 -*-

import logging
import time
import json
import shapely
from datetime import datetime
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

from ckanext.nextgeossharvest.lib.jrc_emis_base import JrcEmisBaseHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)

# sys.setrecursionlimit(3000)

# {
# "start_date":"2020-08-01T00:00:00Z",
# "end_date":"2020-08-01T05:00:00Z",
# }

class JrcEmisHarvester(JrcEmisBaseHarvester, NextGEOSSHarvester,
                        HarvesterBase):
    """A Harvester for JRC Environmental Marine Information System Catalog."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'jrc_emis',
            'title': 'JRC EMIS Harvester',
            'description': 'A Harvester for JRC EMIS Catalog'
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
        log = logging.getLogger(__name__ + '.JrcEmis.gather')
        log.debug('JrcEmisHarvester gather_stage for job: %r', harvest_job)

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

        # username = self.source_config.get('username')
        # password = self.source_config.get('password')

        # start_date = self.source_config.get('start_date', '')
        # end_date = self.source_config.get('end_date', '')

        # if restart_date != '*':
        #     start_date = restart_date

        # if start_date != '*':
        #     time_query = 'sensing_start__gte={}&sensing_start__lte={}'.format(start_date,
        #                                                          end_date)
        # else:
        #     time_query = ''

        url = "https://data.jrc.ec.europa.eu/collection/emis"
        url_prefix = 'https://data.jrc.ec.europa.eu'
        page = requests.get(url)    
        data = page.text
        soup = BeautifulSoup(data)

        datasets = []
        for link in soup.findAll('a', href=True, recursive=False):
            if str(link.get('href')).startswith('/dataset/'):
                datasets.append(url_prefix+link.get('href'))

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

        self.provider = 'emis'

        products = self._get_products(datasets)
        log.debug("Products: {}".format(products))

        ids = self._parse_products(products)

        log.debug("IDs: {}".format(ids))

        return ids

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True

    def _get_products(self, datasets):
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

            for dataset in datasets:
            # print(dataset)
                html_string = datasets[0]
                soup = BeautifulSoup(urlopen(html_string), 'lxml') # Parse the HTML as a string

                table_add_info = soup.findAll('table', href=True, recursive=False)[1]
                table_gen_info = soup.findAll('table', href=True, recursive=False)[2]

                table_add = str(table_add_info).replace('<th', '<td')
                table_add = table_add.replace('\n', '')

                table_add_list = [[cell.text for cell in row("td")]
                                        for row in BeautifulSoup(table_add)("tr")]
                table_add_dict = dict(table_add_list)
                table_add_dict['Language'] = ''.join(table_add_dict['Language'].split())
                table_add_dict['Landing page'] = ''.join(table_add_dict['Landing page'].split())

                # pprint(table_add_dict)
                table_gen = str(table_gen_info).replace('<th', '<td')
                table_gen = table_gen.replace('\n', '')

                table_gen_list = [[cell.text for cell in row("td")]
                                        for row in BeautifulSoup(table_gen)("tr")]
                table_gen_dict = dict(table_gen_list)
                table_gen_dict['Lineage'] = table_gen_dict['Lineage'].replace('\'', '')
                # pprint(table_gen_dict)

                title_info = str(soup.findAll('h3', href=True, recursive=False)[0]).split('<h3>Title: ')[1].split('</h3>')[0]


                table = table_add_dict
                table.update(table_gen_dict)
                uid = {'uid': html_string.split('dataset/')[1]}
                table.update(uid)
                title = {'title': title_info}
                table.update(title)
                # pprint(table)
                products.append(table)

            # pprint(products)

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
        new_counter = 0

        # Create a harvest object for each entry
        for entry in products:
            # Skip wkt and txt files
            # if entry['filename'].endswith(('.wkt', '.txt')):
            #     continue

            entry_guid = entry['uid']
            # entry_name = entry['filename']
            entry_restart_date = entry['Issue date']

            # package = Session.query(Package) \
            #     .filter(Package.name == entry_name).first()

            # if package:
            #     # Meaning we've previously harvested this,
            #     # but we may want to reharvest it now.
            #     previous_obj = model.Session.query(HarvestObject) \
            #         .filter(HarvestObject.guid == entry_guid)

            #     # previous_obj = model.Session.query(HarvestObject) \
            #     #     .filter(HarvestObject.guid == entry_guid) \
            #     #     .filter(HarvestObject.current == True) \
            #     #     .first()  # noqa: E712
            #     if previous_obj:
            #         previous_obj.current = False
            #         previous_obj.save()

            #     if self.update_all:
            #         log.debug('{} already exists and will be updated.'.format(entry_guid))  # noqa: E501
            #         status = 'change'
            #     else:
            #         log.debug('{} will not be updated.'.format(entry_guid))  # noqa: E501
            #         status = 'unchanged'

            #     obj = HarvestObject(guid=entry_guid, job=self.job,
            #                         extras=[HOExtra(key='status',
            #                                 value=status),
            #                                 HOExtra(key='restart_date',
            #                                 value=entry_restart_date)])
            #     obj.content = json.dumps(entry)
            #     obj.package = package
            #     obj.save()
            #     ids.append(obj.id)

            # elif not package:
                # It's a product we haven't harvested before.
            log.debug('{} has not been harvested before. Creating a new harvest object.'.format(entry_name))  # noqa: E501
            obj = HarvestObject(guid=entry_guid, job=self.job,
                                extras=[HOExtra(key='status',
                                        value='new'),
                                        HOExtra(key='restart_date',
                                        value=entry_restart_date)])
            new_counter += 1
            obj.content = json.dumps(entry)
            obj.package = None
            obj.save()
            ids.append(obj.id)

        return

    def _get_spatial_info(self, req, products):
        return