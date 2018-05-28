# -*- coding: utf-8 -*-
import re
from enum import Enum
import logging
import json
import uuid
from datetime import datetime
import time
from bs4 import BeautifulSoup

from sqlalchemy import desc

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import Timeout

from ckan.common import config
from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.opensearch_base import OpenSearchHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckan import model
from ckan.model import Package

from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase


from probav_collections import COLLECTION_DESCRIPTIONS

log = logging.getLogger(__name__)


COLLECTION_TEMPLATE = 'PROBAV_{type}_{resolution}_V001'
COLLECTIONS = [
    "PROBAV_L2A_100M_V001"
    "PROBAV_L2A_1KM_V001",
    "PROBAV_L2A_333M_V001",
    "PROBAV_P_V001",
    "PROBAV_S1-TOA_100M_V001",
    "PROBAV_S1-TOA_1KM_V001",
    "PROBAV_S1-TOA_333M_V001",
    "PROBAV_S1-TOC_100M_V001",
    "PROBAV_S1-TOC_1KM_V001",
    "PROBAV_S1-TOC_333M_V001",
    "PROBAV_S1-TOC-NDVI_100M_V001",
    "PROBAV_S10-TOC_1KM_V001",
    "PROBAV_S10-TOC_333M_V001",
    "PROBAV_S10-TOC-NDVI_1KM_V001",
    "PROBAV_S10-TOC-NDVI_333M_V001",
    "PROBAV_S5-TOA_100M_V001",
    "PROBAV_S5-TOC_100M_V001",
    "PROBAV_S5-TOC-NDVI_100M_V001",
]


log = logging.getLogger(__name__)

class Units(Enum):
    METERS = 'M'
    KILOMETERS = 'K'


class ProductType(Enum):
    TOC = 'TOA'
    TOC = 'TOC'
    L2A = 'L2A'


class Resolution(object):

    def __init__(self, value, units):
        self.value = value
        self.units = units

    def __str__(self):
        return str(self.value) + self.units.value


class ProbaVCollection(object):

    def __init__(self, product_type, resolution):
        self.product_type = product_type
        self.resolution = resolution


class L2AProbaVCollection(ProbaVCollection):

    def _type_token(self):
        return 'L2A'

    def get_name(self):
        return 'Proba-V Level-2A ({})'.format(self.resolution)

    def get_description(self):
        return COLLECTION_DESCRIPTIONS[self.get_name()]

    def get_tags(self):
        return ['Proba-V', 'L2A', str(self.resolution)]

    def __str__(self):
        return 'PROBAV_{}_{}_V001'.format(self._type_token(), str(self.resolution))


class SProbaVCollection(ProbaVCollection):

    def __init__(self, frequency, product_type, resolution, ndvi):
        super(SProbaVCollection, self).__init__(product_type, resolution)
        self.frequency = frequency
        self.ndvi = ndvi

    def _type_token(self):
        return 'S{}-{}{}'.format(str(self.frequency),
                                 str(self.product_type),
                                 ('NDVI' if self.ndvi else '')
                                 )

    def get_name(self):
        return ''

    def get_description(self):
        return ''

    def get_tags(self):
        return []


class PROBAVHarvester(OpenSearchHarvester, NextGEOSSHarvester):
    """
    A an example of how to build a harvester for OpenSearch sources.

    You'll want to add some custom code (or preferably a custom class) to
    handle parsing the entries themselves, as well as any special logic
    for deciding which entries to import, etc.
    """
    implements(IHarvester)

    def info(self):
        return {
            'name': 'proba-v',
            'title': 'Proba-V Harvester',
            'description': 'A Harvester for Proba-V Products'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            # If your harvester has a config,
            # validate it here.

        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.{your_harvester}.gather')
        log.debug('{your_harvester} gather_stage for job: %r', harvest_job)
        self.job = harvest_job
        self._set_source_config(self.job.source.config)
        self.os_id_name = 'atom:id'  # Example
        self.os_id_attr = {'key': None}  # Example
        self.os_guid_name = 'atom:id'  # Example
        self.os_guid_attr = {'key': None}  # Example
        self.os_restart_date_name = 'atom:updated'
        self.os_restart_date_attr = {'key': None}
        self.flagged_extra = None

        # This will be the URL that you'll begin harvesting from
        # harvest_url = 'http://www.vito-eodata.be/openSearch/findProducts.atom?collection=urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001&platform=PV01&start=2018-01-01&end=2018-01-02&count=500'
        harvest_url = "http://www.vito-eodata.be/openSearch/findProducts.atom?collection=urn:ogc:def:EOP:VITO:PROBAV_S1-TOA_100M_V001&platform=PV01&start=2018-01-01&end=2018-01-02&count=500"
        
        log.debug('Harvest URL is {}'.format(harvest_url))

        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()
        self.provider = ''  # a string indicating the source to be used in the logs
        # (could be set in the config)
        # As explained above, only harvest_url is required.

        config = json.loads(harvest_job.source.config)
        auth = (config.get('ckanext.nextgeossharvest.nextgeoss_username'),
                config.get('ckanext.nextgeossharvest.nextgeoss_password'))
        auth = ('nextgeoss', 'nextgeoss')
        ids = self._crawl_results(harvest_url, timeout=60, parser='lxml-xml', gather_entry=self._gather_metalink_files, auth=auth)

        print(ids)

        return ids

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""

        # We don't need to fetch anything—the OpenSearch entries contain all
        # the content we need for the import stage.

        return True

    def _parse_content(self, content_str):
        content = BeautifulSoup(content_str, 'lxml-xml')
        identifier = self._parse_identifier_element(content)
        collection = self._parse_collection_from_identifier(identifier)
        parsed_content = {}
        parsed_content['title'] = collection.get_name()
        parsed_content['description'] = collection.get_description()
        parsed_content['tags'] = self._create_ckan_tags(collection.get_tags())
        parsed_content['identifier'] = self._parse_identifier(identifier)
        parsed_content['uuid'] = str(uuid.uuid4())
        parsed_content['StartTime'], parsed_content['StopTime'] = self._parse_interval(
            content)
        parsed_content['Collection'] = str(collection)
        parsed_content['name'] = self._parse_name(identifier)
        parsed_content['filename'] = self._parse_filename(identifier)
        parsed_content['spatial'] = json.dumps(self._bbox_to_geojson(
            self._parse_bbox(content)))
        parsed_content['notes'] = parsed_content['description']
        parsed_content['metadata_download'] = self._get_metadata_url(content)
        parsed_content['product_download'] = self._get_product_url(content)
        parsed_content['thumbnail_download'] = self._get_thumbnail_url(content)
        return parsed_content

    def _create_ckan_tags(self, tags):
        return [{'name': tag} for tag in tags]

    def _parse_identifier_element(self, entry):
        return entry.find('identifier').string

    def _parse_identifier(self, identifier):
        identifier_parts = identifier.split(':')
        return '{}_{}'.format(identifier_parts[-2], identifier_parts[-1])

    def _parse_interval(self, entry):
        date_str = str(entry.find('date').string)
        return date_str.split('/')

    def _parse_name(self, identifier):
        identifier_parts = identifier.split(':')
        name = identifier_parts[-2]
        return '{}_{}'.format(name, identifier_parts[-1]).lower()

    def _parse_filename(self, identifier):
        identifier_parts = identifier.split(':')
        filename = identifier_parts[-2]
        return '{}_{}.HDF5'.format(filename, identifier_parts[-1])

    def _bbox_to_geojson(self, bbox):
        return {
            'type': 'Polygon',
            'crs': {
                'type': 'EPSG',
                'properties': {
                    'coordinate_order': 'Long,Lat',
                    'code': 4326
                },
            },
            'coordinates': [self._bbox_to_polygon(bbox)]
        }
    
    def _bbox_to_polygon(self, bbox):
        lat_min, lng_min, lat_max, lng_max = bbox
        return [[lng_min,lat_max],
                [lng_max,lat_max],
                [lng_max,lat_min],
                [lng_min, lat_min],
                [lng_min,lat_max]]

    def _parse_bbox(self, entry):
        bbox_str = entry.box.string
        bbox_parts = bbox_str.split()
        return [float(coord) for coord in bbox_parts]

    def _parse_collection_from_identifier(self, identifier):
        collection_name = identifier.split(':')[5]
        _, product_type, resolution_str, _ = collection_name.split('_')
        resolution = self._parse_resolution(resolution_str)
        if product_type == 'L2A':
            return L2AProbaVCollection(ProductType.L2A, resolution)
        else:
            product_parts = product_type.split('-')
            frequency = int(product_parts[0][1:])
            subtype = ProductType(product_parts[1])
            ndvi = len(product_parts) > 0 and product_parts[2] == 'NDVI'
            return SProbaVCollection(frequency, subtype, resolution, ndvi)

    def _parse_resolution(self, resolution_str):
        # we are assuming resolution is one of {100M, 1Km, 333M}
        if resolution_str.endswith('KM'):
            units = Units.KILOMETERS
            value = int(resolution_str[:-2])
        else:
            units = Units.METERS
            value = int(resolution_str[:-1])
        return Resolution(value, units)

    def _get_resources(self, parsed_content):
        return [
            {
                'name': 'Metadata Download',
                'url': parsed_content['metadata_download'],
                'format': 'xml',
                'mimetype': 'application/xml'
            },
            {
                'name': 'Product Download',
                'url': parsed_content['product_download'],
                'format': 'hdf5',
                'mimetype': 'application/x-hdf5'
            },
            {
                'name': 'Thumbnail Download',
                'url': parsed_content['thumbnail_download'],
                'format': 'png',
                'mimetype': 'image/png'
            }
        ]

    def _get_metadata_url(self, content):
        return str(content.find('link', title='HMA')['href'])

    def _get_product_url(self, content):
        return str(content.find('link', rel='enclosure')['href'])

    def _get_thumbnail_url(self, content):
        return str(content.find('link', rel='icon')['href'])
    
    def _get_url(self, url, auth=None, **kwargs):
        log.info('getting %s with user %s', url, auth[0])
        if auth:
            kwargs['auth'] = HTTPBasicAuth(*auth)
        response = requests.get(url, **kwargs)
        log.info('got HTTP %d', response.status_code)
        return response

    def _crawl_results(self, harvest_url, limit=100, timeout=5, auth=None, provider=None, parser='lxml', gather_entry=None):  # noqa: E501
        """
        Iterate through the results, create harvest objects,
        and return the ids.
        """
        ids = []
        if gather_entry is None:
            gather_entry = self._gather_entry


        while len(ids) < limit and harvest_url:
            # We'll limit ourselves to one request per second
            start_request = time.time()

            # Make a request to the website
            timestamp = str(datetime.utcnow())
            log_message = '{:<12} | {} | {} | {}s'
            try:
                kwargs = { 'verify': False,
                           'timeout': timeout }
                r = self._get_url(harvest_url, auth=auth, **kwargs)
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

            soup = BeautifulSoup(r.content, parser)

            # Get the URL for the next loop, or None to break the loop
            harvest_url = self._get_next_url(soup)
            log.debug('next url: %s', harvest_url)

            # Get the entries from the results
            entries = self._get_entries_from_results(soup)
            log.debug('OpenSearch entries: %d', len(entries))


            # Create a harvest object for each entry
            for entry in entries:
                ids.extend(gather_entry(entry, auth=auth))
            

            end_request = time.time()
            request_time = end_request - start_request
            if request_time < 1.0:
                time.sleep(1 - request_time)

        return ids

    def _gather_metalink_files(self, opensearch_entry, auth=None):
        content = opensearch_entry['content']
        content_soup = BeautifulSoup(content, 'lxml-xml')
        metalink_url = self._parse_metalink_url(content_soup)
        response = self._get_url(metalink_url, auth)
        metalinks = BeautifulSoup(response.text, 'lxml-xml')
        ids = list()
        for _file in metalinks.find_all('file'):
            metalink_content = self._merge_contents(content, str(_file))
            opensearch_entry['content'] = metalink_content
            id_list = self._gather_entry(opensearch_entry)
            ids.extend(id_list)
        return ids

    def _parse_metalink_url(self, openseach_entry):
        return openseach_entry.find('link', type="application/metalink+xml")['href']

            
    def _merge_contents(self, content, file_content):
        content_dict = {'opensearch_entry': content,
                        'file_element': file_content}
        return json.dumps(content_dict)

    def _gather_entry(self, entry):
        # Create a harvest object for each entry
        entry_guid = entry['guid']
        log.debug('gathering %s', entry_guid)
        entry_name = entry['identifier']
        entry_restart_date = entry['restart_date']

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
            # E.g., a Sentinel dataset exists,
            # but doesn't have a NOA resource yet.
            elif self.flagged_extra and not self._get_package_extra(package.as_dict(), self.flagged_extra):  # noqa: E501
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
            return [obj.id]
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
            return [obj.id]