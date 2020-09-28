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

from ckanext.nextgeossharvest.lib.noa_groundsegment_base import NoaGroundsegmentBaseHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class NoaGroundsegmentHarvester(NoaGroundsegmentBaseHarvester, NextGEOSSHarvester,
                        HarvesterBase):
    """A Harvester for Noa Groundsegment Products."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'noa_groundsegment',
            'title': 'NOA Groundsegment Harvester',
            'description': 'A Harvester for NOA Groundsegment Products'
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
        log = logging.getLogger(__name__ + '.NoaGroundsegment.gather')
        log.debug('NoaGroundSegmentHarvester gather_stage for job: %r', harvest_job)

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
                # Convert _get_object_extra datetime to the API datetime format
                restart_dt = datetime.strptime(restart_date, "%Y-%m-%dT%H:%M:%S")
                restart_date = restart_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except IndexError:
                restart_date = '*'
            except ValueError:
                # MERSI products throw this error due to different datetime format
                # Change format and subtract one second to account for rounding
                restart_dt = datetime.strptime(restart_date, "%Y-%m-%dT%H:%M:%S.%f") + timedelta(seconds=-1)
                restart_date = restart_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            restart_date = '*'

        log.debug('Restart date is {}'.format(restart_date))

        username = self.source_config.get('username')
        password = self.source_config.get('password')

        start_date = self.source_config.get('start_date', '')
        end_date = self.source_config.get('end_date', '')

        # Set the limit for the maximum number of pages per job.
        # Since the new harvester jobs will be created on a rolling basis
        # via cron jobs, we don't need to grab all the results from a date
        # range at once and the harvester will resume from the last gathered
        # date each time it runs.
        # Each page corresponds to 100 products
        page_timeout = int(self.source_config.get('page_timeout', '5'))

        if restart_date != '*':
            start_date = restart_date

        if start_date != '*':
            time_query = 'sensing_start__gte={}&sensing_start__lte={}'.format(start_date,
                                                                 end_date)
        else:
            time_query = ''

        harvest_url = 'https://groundsegment.space.noa.gr/api/products?{}'.format(time_query)

        # log.debug('Harvest URL: {}'.format(harvest_url))

        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()

        if not hasattr(self, 'harvester_logger'):
            self.harvester_logger = self.make_harvester_logger()

        self.provider = 'noa_groundsegment'

        products = self._get_products(harvest_url, username, password, page_timeout)

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

    def _get_products(self, harvest_url, username, password, page_timeout):
        """
        Create a session and return the results
        """

        # Create requests session
        req = requests.Session()
        req.auth = (username, password)
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
            # Skip wkt and txt files
            if entry['filename'].endswith(('.wkt', '.txt')):
                continue

            entry_guid = entry['filename']
            entry_name = entry['filename']
            entry_restart_date = entry['sensing_start']

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
        Gets the spatial information for every product
        """
        product_list = []
        temp_reception_id = 0

        reception_url = 'https://groundsegment.space.noa.gr/api/receptions?id='

        # Add spatial data for every product
        # Requires new call to API
        for product in products:

            # Products are sorted by reception_id and filename
            # By getting the spatial information from the 1st product in the same reception
            # we avoid caling the API for every product
            if temp_reception_id != product['reception_id']:
                temp_reception_id = product['reception_id']

                # Wait for 2 seconds before calling the API to avoid possible 403 errors
                # in case too many requests need to be done
                time.sleep(2)
                # Api call for geometry
                spatial_wkb = (req.get(reception_url + product['reception_id'])).json()['results'][0]['geom']
                
                if spatial_wkb is not None:
                    # Convert wkb to wkt
                    spatial_shpl = shapely.wkb.loads(spatial_wkb, hex=True)
                    spatial_wkt = spatial_shpl.wkt

                    # wkt to geojson
                    spatial_geojson = self._convert_to_geojson(spatial_wkt)
                    product["spatial"] = spatial_geojson
                else:
                    # Some older receptions have a null geometry
                    # In this case a geometry of the supported region is added
                    spatial_wkt = "POLYGON((-7.738739221402264 52.307731872498174,45.17141702859774 52.307731872498174, 45.17141702859774 28.361326991015748,-7.738739221402264 28.361326991015748, -7.738739221402264 52.307731872498174))"

                    spatial_geojson = self._convert_to_geojson(spatial_wkt)
                    product["spatial"] = spatial_geojson
            else:
                product["spatial"] = spatial_geojson
            
            product_list.append(product)

        return product_list