 # -*- coding: utf-8 -*-

import logging
import time
import json
import shapely
from datetime import datetime, timedelta

import requests
from requests.exceptions import Timeout

from sqlalchemy import desc

from ckan import model
from ckan.model import Package

from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.noa_geobservatory_base import NoaGeobservatoryBaseHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class NoaGeobservatoryHarvester(NoaGeobservatoryBaseHarvester, NextGEOSSHarvester,
                        HarvesterBase):
    """A Harvester for Noa Geobservatory Products."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'noa_geobservatory',
            'title': 'NOA Geobservatory Harvester',
            'description': 'A Harvester for NOA Geobservatory Products'
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
                    raise ValueError('start_date format must be 2020-01-01T00:00:00Z')  # noqa: E501
            else:
                raise ValueError('start_date is required, the format must be 2020-01-01T00:00:00Z')  # noqa: E501

            if 'end_date' in config_obj:
                try:
                    datetime.strptime(config_obj['end_date'],
                                      '%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    raise ValueError('end_date format must be 2020-01-01T00:00:00Z')  # noqa: E501

            if 'page_timeout' in config_obj:
                timeout = config_obj['page_timeout']
                if not isinstance(timeout, int) and not timeout > 0:
                    raise ValueError('page_timeout must be a positive integer')

            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')

            if type(config_obj.get('update_all', False)) != bool:
                raise ValueError('update_all must be true or false')

        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.NoaGeobservatory.gather')
        log.debug('NoaGeobservatoryHarvester gather_stage for job: %r', harvest_job)

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
                restart_date = '*'

        else:
            restart_date = '*'

        log.debug('Restart date is {}'.format(restart_date))

        start_date = self.source_config.get('start_date', '')
        end_date = self.source_config.get('end_date', '')

        # Set the limit for the maximum number of pages per job.
        # Since the new harvester jobs will be created on a rolling basis
        # via cron jobs, we don't need to grab all the results from a date
        # range at once and the harvester will resume from the last gathered
        # date each time it runs.
        # Each page corresponds to 100 products
        page_timeout = int(self.source_config.get('page_timeout', '2'))

        if restart_date != '*':
            start_date = restart_date

        if start_date != '*':
            time_query = 'master__gte={}&master__lte={}'.format(start_date, end_date)
        else:
            time_query = ''

        harvest_url = 'http://geobservatory.beyond-eocenter.eu/api/interferograms?{}'.format(time_query)

        #log.debug('Harvest URL: {}'.format(harvest_url))

        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()

        if not hasattr(self, 'harvester_logger'):
            self.harvester_logger = self.make_harvester_logger()

        self.provider = 'noa_geobservatory'

        products = self._get_products(harvest_url, page_timeout)

        ids = self._parse_products(products)

        return ids

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True

    def _build_products(self, products, req, page_timeout):
        """Handles pagination"""

        # Counter starts from 1 due to one call happening in the _get_products function
        page_counter = 1

        while products['next'] and page_counter < page_timeout:
            for product in products['results']:
                yield product
            
            req.get(products['next']).raise_for_status()
            products = req.get(products['next']).json()

            page_counter += 1
            time.sleep(2)

        for product in products['results']:
            yield product

    def _get_products(self, harvest_url, page_timeout):
        """
        Create a session and return the results
        """

        # Create requests session
        req = requests.Session()
        req.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json;charset=UTF-8',})

        # Make a request to the website
        timestamp = str(datetime.utcnow())
        log_message = '{:<12} | {} | {} | {}s'
        try:
            status_code = req.get(harvest_url).status_code
            products_json = (req.get(harvest_url)).json()

            # Get the products
            products = self._build_products(products_json, req, page_timeout)

            # Add spatial information to every product
            product_list = self._get_spatial_info(req, products)

            return product_list

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

            entry_guid = entry['imgtif'].split('/')[1].lower() + "_" + entry['type'] + "_" + str(entry['intid'])
            entry_name = entry['imgtif'].split('/')[1].lower() + "_" + entry['type'] + "_" + str(entry['intid'])
            entry_restart_date = entry['master']

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
                obj.content = json.dumps(entry)
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
                obj.content = json.dumps(entry)
                obj.package = None
                obj.save()
                ids.append(obj.id)


        harvester_msg = '{:<12} | {} | jobID:{} | {} | {}'
        if hasattr(self, 'harvester_logger'):
            timestamp = str(datetime.utcnow())
            self.harvester_logger.info(harvester_msg.format(self.provider,
                                        timestamp, self.job.id, new_counter, 0))  # noqa: E128, E501

        return ids

    def _get_spatial_info(self, req, products):
        """
        Creates the spatial information for every product
        """
        product_list = []

        # Add spatial data for every product
        for product in products:
            
            # Create WKT
            spatial_wkt = "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))".format(
                product['west'], product['north'], 
                product['east'], product['north'], 
                product['east'], product['south'], 
                product['west'], product['south'], 
                product['west'], product['north']
            )
            
            # WKT to geojson
            spatial_geojson = self._convert_to_geojson(spatial_wkt)
            product["spatial"] = spatial_geojson
            
            product_list.append(product)

        return product_list