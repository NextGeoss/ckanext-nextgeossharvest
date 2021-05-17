 # -*- coding: utf-8 -*-

import logging
import time
import json
import shapely
import itertools
import re
import stringcase
from datetime import datetime

import requests
from requests.exceptions import Timeout
from requests.auth import HTTPBasicAuth

from sqlalchemy import desc

from ckan import model
from ckan.model import Package

from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.noa_epidemics_base import NoaEpidemicsBaseHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class NoaEpidemicsHarvester(NoaEpidemicsBaseHarvester, NextGEOSSHarvester,
                        HarvesterBase):
    """A Harvester for Noa Epidemics Products."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'noa_epidemics',
            'title': 'NOA Epidemics Harvester',
            'description': 'A Harvester for NOA Epidemics Products'
        }

    def validate_config(self, config):
        if not config:
            raise ValueError('Must include one mosquito_type. Supported types: aedes, culex, anopheles.')  # noqa: E501

            #return config

        try:
            config_obj = json.loads(config)

            if 'start_date' in config_obj:
                try:
                    datetime.strptime(config_obj['start_date'], '%Y-%m-%d')
                except ValueError:
                    raise ValueError('start_date format must be 2018-01-01')  # noqa: E501
            #else:
            #    raise ValueError('start_date is required, the format must be 2018-01-01T00:00:00Z')  # noqa: E501

            if 'end_date' in config_obj:
                try:
                    datetime.strptime(config_obj['end_date'], '%Y-%m-%d')
                except ValueError:
                    raise ValueError('end_date format must be 2018-01-01')  # noqa: E501

            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                if not isinstance(timeout, int) and not timeout > 0:
                    raise ValueError('timeout must be a positive integer')

            if 'datasets_per_job' in config_obj:
                limit = config_obj['datasets_per_job']
                if not isinstance(limit, int) and not limit > 0:
                    raise ValueError('datasets_per_job must be a positive integer')  # noqa: E501
            
            if 'mosquito_type' in config_obj:
                mosquito_type = config_obj['mosquito_type']

                if mosquito_type not in ['aedes', 'culex', 'anopheles']:
                    raise ValueError('Must include one supported mosquito_type. Supported types: aedes, culex, anopheles.')

            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')

            if type(config_obj.get('update_all', False)) != bool:
                raise ValueError('update_all must be true or false')

        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.NoaEpidemics.gather')
        log.debug('NoaEpidemicsHarvester gather_stage for job: %r', harvest_job)

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

        # Get config
        start_date = self.source_config.get('start_date', '')
        end_date = self.source_config.get('end_date', '')
        mosquito_type = self.source_config.get('mosquito_type', '')
        limit = self.source_config.get('datasets_per_job', 100)

        # Time related changes
        if restart_date != '*':
            start_date = restart_date

        if mosquito_type == 'aedes' and end_date:
            end_date = '00' + end_date[2:]

        if start_date != '*':
            if mosquito_type == 'aedes' and start_date:
                start_date = '00' + start_date[2:]

            time_query = 'dt_placement__gte={}&dt_placement__lt={}'.format(start_date, end_date)
        else:
            time_query = ''

        # URL with mosquito type and time query
        base_url = 'http://epidemics.space.noa.gr/api_v2'

        harvest_url = "{}/{}?{}".format(base_url, mosquito_type, time_query)

        log.debug('Harvest URL: {}'.format(harvest_url))

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

        self.provider = 'noa'

        products = self._get_products(harvest_url, limit)

        ids = self._parse_products(products, mosquito_type)

        # log.debug("IDs: {}".format(ids))

        return ids

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True

    def _get_entries_from_results(self, json_result):
        """Extract the entries from an OpenSearch response."""

        # All datasets in SIMOcean catalogue belong into two groups,
        # the first is an encompassing group (in-situ, model or satellite)
        # that "hosts" the other groups (collections)
        # In this harvester, only the 'in-situ' and 'model' groups are to
        # be harvested, since 'satellite' (CMEMS) is already being collected by
        # a different harvester
        group_list = ['satellite']

        entries = []

        for entry in json_result['result']['results']:
            content = entry
            identifier = entry['name']
            guid = entry['id']
            restart_date = entry['metadata_modified']
            if restart_date[-1] != 'Z':
                restart_date = restart_date + 'Z'

            group_allowed = False
            for group in entry['groups']:
                if group['name'] in group_list:
                    group_allowed = True

            if group_allowed:
                entries.append({'content': content, 'identifier': identifier,
                                'guid': guid, 'restart_date': restart_date})

        return entries

    def _build_products(self, products, req):
        """Handles pagination"""
        while products['next']:
            for product in products['results']:
                yield product
            
            req.get(products['next']).raise_for_status()
            products = req.get(products['next']).json()

            time.sleep(2)

        for product in products['results']:
            yield product

    def _get_products(self, harvest_url, limit):
        """
        Create a session and return the results
        """

        req = requests.Session()
        req.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json;charset=UTF-8',})
        
        # Make a request to the website
        timestamp = str(datetime.utcnow())
        log_message = '{:<12} | {} | {} | {}s'
        try:
            status_code = req.get(harvest_url).status_code
            products_json = (req.get(harvest_url)).json()

            # Get the products (returns generator)
            products = self._build_products(products_json, req)

            # Apply limit on product number
            products = itertools.islice(products, limit)

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

    def _parse_products(self, products, mosquito_type):  # noqa: E501
        """
        Iterate through the results, create harvest objects,
        and return the ids.
        """
        ids = []
        new_counter = 0

        # Create a harvest object for each entry
        for entry in products:
            # Add mosquito type on object
            entry['mosquito_type'] = mosquito_type

            # Correct Date
            if entry['dt_placement'].startswith('00'):
                entry['dt_corrected'] = '20' + entry['dt_placement'][2:]
        
                filename = "{}_{}_{}".format(mosquito_type, entry['station_id'], entry['dt_corrected'])

            else:
                filename = "{}_{}_{}".format(mosquito_type, entry['station_id'], entry['dt_placement'])

            # Sanitize filename
            filename = self._sanitize_filename(filename)

            # Add coast_mean on aedes for uniqueness
            if mosquito_type == 'aedes':
                filename = filename + '_' + str(int(entry['coast_mean_dist_1000']))

            entry_guid = filename
            entry_name = filename
            entry['filename'] = filename

            entry_restart_date = entry['dt_placement']

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

        reception_url = 'http://epidemics.space.noa.gr/api_v2/locations/?station_id='

        # Add spatial data for every product
        # Requires new call to API
        for product in products:
            
            # Api call for geometry (Latitude, Longitude)
            spatial_info = (req.get(reception_url + product['station_id'])).json()['results'][0]
            latitude = spatial_info['latitude']
            longitude = spatial_info['longitude']

            # Points to Polygon
            # The accuracy of the Points has already been reduced for safety reasons
            # We need to create a Polygon to map a wider area
            spatial_wkt = "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))".format(
                longitude - 0.05, latitude + 0.05, 
                longitude - 0.05, latitude - 0.05, 
                longitude + 0.05, latitude - 0.05, 
                longitude + 0.05, latitude + 0.05,
                longitude - 0.05, latitude + 0.05
            )
        
            # WKT to geojson
            spatial_geojson = self._convert_to_geojson(spatial_wkt)
            product["spatial"] = spatial_geojson

            product_list.append(product)

        return product_list

    def _sanitize_filename(self, filename):
        """ Sanitizes filename/identifier.
            Keeps only alphanumeric characters.
        """

        filename = re.sub('[^0-9a-zA-Z]+', '_', filename).lower()
        sc_filename = stringcase.snakecase(filename).strip("_")
        while "__" in sc_filename:
            sc_filename = sc_filename.replace("__", "_")
        return sc_filename