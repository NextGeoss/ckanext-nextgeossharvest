# -*- coding: utf-8 -*-
import re
from enum import Enum
import logging
import json
import uuid
from datetime import datetime

from bs4 import BeautifulSoup

from sqlalchemy import desc

from ckan.common import config
from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.opensearch_base import OpenSearchHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from probav_collections import COLLECTION_DESCRIPTIONS

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
        harvest_url = 'http://www.vito-eodata.be/openSearch/findProducts.atom?collection=urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001&platform=PV01&start=2018-01-01&end=2018-01-02&count=500'
        log.debug('Harvest URL is {}'.format(harvest_url))

        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()
        self.provider = ''  # a string indicating the source to be used in the logs
        # (could be set in the config)
        # As explained above, only harvest_url is required.
        ids = self._crawl_results(harvest_url, timeout=60, parser='lxml-xml')

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
        parsed_content['spatial'] = self._bbox_to_geojson(
            self._parse_bbox(content))
        parsed_content['notes'] = parsed_content['description']
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
        return '{}_{}.HDF5'.format(name, identifier_parts[-1])

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

    def _get_resources(self, content):
        return [
            {
                'name': 'Metadata Download',
                'url': self._get_metadata_url(content),
                'format': 'xml',
                'mimetype': 'application/xml'
            },
            {
                'name': 'Product Download',
                'url': self._get_product_url(content),
                'format': 'hdf5',
                'mimetype': 'application/x-hdf5'
            },
            {
                'name': 'Thumbnail Download',
                'url': self._get_thumbnail_url(content),
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
