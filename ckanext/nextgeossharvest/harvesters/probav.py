# -*- coding: utf-8 -*-
import re
from enum import Enum
import logging
import json
import uuid
from datetime import datetime, timedelta
import time
from bs4 import BeautifulSoup
from os import path
from urllib import urlencode, unquote
from urlparse import urlparse, urlunparse, parse_qsl
from sqlalchemy import desc

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import Timeout

from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.opensearch_base import OpenSearchHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckan.model import Package

from ckanext.harvest.model import HarvestObjectExtra as HOExtra

from probav_collections import COLLECTION_DESCRIPTIONS

log = logging.getLogger(__name__)

COLLECTION_TEMPLATE = 'PROBAV_{type}_{resolution}_V001'

URL_TEMPLATE = 'http://www.vito-eodata.be/openSearch/findProducts.atom?collection=urn:ogc:def:EOP:VITO:{}&platform=PV01&start={}&end={}&count=500'  # count=500  # noqa: E501
DATE_FORMAT = '%Y-%m-%d'

log = logging.getLogger(__name__)


class Units(Enum):
    METERS = 'M'
    KILOMETERS = 'KM'


class ProductType(Enum):
    TOA = 'TOA'
    TOC = 'TOC'
    L2A = 'L2A'
    L1C = 'P'


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

    def get_description(self):
        return COLLECTION_DESCRIPTIONS[self.get_name()]

    def get_tags(self):
        return ['Proba-V', self._type_token(), str(self.resolution)]

    def __str__(self):
        return 'PROBAV_{}_{}_V001'.format(self._type_token(),
                                          str(self.resolution))


class ProbaVCollectionP(object):
    def __init__(self, product_type):
        self.product_type = product_type

    def get_description(self):
        return COLLECTION_DESCRIPTIONS[self.get_name()]

    def get_tags(self):
        return ['Proba-V', 'L1C']

    def __str__(self):
        return 'PROBAV_{}_V001'.format(self._type_token())


class L1CProbaVCollection(ProbaVCollectionP):
    def _type_token(self):
        return 'P'

    def get_name(self):
        return 'Proba-V Level-1C'


class L2AProbaVCollection(ProbaVCollection):
    def _type_token(self):
        return 'L2A'

    def get_name(self):
        return 'Proba-V Level-2A ({})'.format(self.resolution)


class SProbaVCollection(ProbaVCollection):
    def __init__(self, frequency, product_type, resolution, ndvi):
        super(SProbaVCollection, self).__init__(product_type, resolution)
        self.frequency = frequency
        self.ndvi = ndvi

    def _type_token(self):
        return 'S{}-{}{}'.format(
            str(self.frequency), self.product_type.value,
            ('-NDVI' if self.ndvi else ''))

    def get_name(self):
        return 'Proba-V S{}-{}{} ({})'.format(
            str(self.frequency), self.product_type.value,
            (' NDVI' if self.ndvi else ''), str(self.resolution))


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

            if 'start_date' in config_obj:
                try:
                    start_date = config_obj['start_date']
                    if start_date != 'YESTERDAY':
                        start_date = datetime.strptime(start_date, '%Y-%m-%d')
                    else:
                        start_date = self.convert_date_config(start_date)
                except ValueError:
                    raise ValueError("start_date must have the format yyyy-mm-dd or be the string 'YESTERDAY'")  # noqa: E501
            else:
                raise ValueError('start_date is required')
            if 'end_date' in config_obj:
                try:
                    end_date = config_obj['end_date']
                    if end_date != 'TODAY':
                        end_date = datetime.strptime(end_date, '%Y-%m-%d')
                    else:
                        end_date = self.convert_date_config(end_date)
                except ValueError:
                    raise ValueError("end_date must have the format yyyy-mm-dd or be the string 'TODAY'")  # noqa E501
            else:
                end_date = self.convert_date_config('TODAY')
            if not end_date > start_date:
                raise ValueError('end_date must be after start_date')
            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                if not isinstance(timeout, int) and not timeout > 0:
                    raise ValueError('timeout must be a positive integer')
            if type(config_obj.get('password', None)) != unicode:
                raise ValueError('password is required and must be a string')
            if type(config_obj.get('username', None)) != unicode:
                raise ValueError('username is required and must be a string')
            if config_obj.get('collection') not in {"PROBAV_S1-TOA_1KM_V001", "PROBAV_S1-TOC_1KM_V001", "PROBAV_P_V001",  # noqa E501
                                                     "PROBAV_S10-TOC_1KM_V001", "PROBAV_S10-TOC-NDVI_1KM_V001",  # noqa E501
                                                     "PROBAV_S1-TOA_100M_V001", "PROBAV_S1-TOC-NDVI_100M_V001",  # noqa E501
                                                     "PROBAV_S5-TOC-NDVI_100M_V001", "PROBAV_S5-TOA_100M_V001",  # noqa E501
                                                     "PROBAV_S5-TOC_100M_V001", "PROBAV_S1-TOC_100M_V001",  # noqa E501
                                                     "PROBAV_S1-TOA_333M_V001", "PROBAV_S1-TOC_333M_V001",  # noqa E501
                                                     "PROBAV_S10-TOC_333M_V001", "PROBAV_S10-TOC-NDVI_333M_V001",  # noqa E501
                                                     "PROBAV_L2A_1KM_V001", "PROBAV_L2A_100M_V001", "PROBAV_L2A_333M_V001"}:  # noqa E501
                raise ValueError('''collections_type is required and must be
                "PROBAV_P_V001", "PROBAV_S1-TOA_1KM_V001",
                "PROBAV_S1-TOC_1KM_V001", "PROBAV_S10-TOC_1KM_V001",
                "PROBAV_S10-TOC-NDVI_1KM_V001", "PROBAV_S1-TOA_100M_V001",
                "PROBAV_S1-TOC-NDVI_100M_V001",
                "PROBAV_S5-TOC-NDVI_100M_V001", "PROBAV_S5-TOA_100M_V001",
                "PROBAV_S5-TOC_100M_V001", "PROBAV_S1-TOC_100M_V001",
                "PROBAV_S1-TOA_333M_V001", "PROBAV_S1-TOC_333M_V001",
                "PROBAV_S10-TOC_333M_V001", "PROBAV_S10-TOC-NDVI_333M_V001",
                "PROBAV_L2A_1KM_V001", "PROBAV_L2A_100M_V001"
                 or "PROBAV_L2A_333M_V001"''')
            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')
            if type(config_obj.get('update_all', False)) != bool:
                raise ValueError('update_all must be true or false')
        except ValueError as e:
            raise e

        return config

    def convert_date_config(self, term):
        """Convert a term into a datetime object."""
        if term == 'YESTERDAY':
            date_time = datetime.now() - timedelta(days=1)
        elif term in {'TODAY', 'NOW'}:
            date_time = datetime.now()

        return date_time.replace(hour=0, minute=0, second=0, microsecond=0)

    def _get_dates_from_config(self, config):

        start_date_str = config['start_date']
        if start_date_str != 'YESTERDAY':
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        else:
            start_date = self.convert_date_config(start_date_str)

        if 'end_date' in config:
            end_date_str = config['end_date']
            if end_date_str != 'TODAY':
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            else:
                end_date = self.convert_date_config(end_date_str)
            if start_date + timedelta(days=1) != end_date:
                end_date = start_date + timedelta(days=1)
        else:
            end_date = start_date + timedelta(days=1)

        return start_date, end_date

    def _init(self):
        self.os_id_name = 'atom:id'  # Example
        self.os_id_attr = {'key': None}  # Example
        self.os_guid_name = 'atom:id'  # Example
        self.os_guid_attr = {'key': None}  # Example
        self.os_restart_date_name = 'atom:updated'
        self.os_restart_date_attr = {'key': None}
        self.flagged_extra = None

    # TODO: define self.provider in logs
    def gather_stage(self, harvest_job):
        self._init()
        self.job = harvest_job
        self._set_source_config(self.job.source.config)
        log.debug('ProbaV Harvester gather_stage for job: %r', harvest_job)

        self.provider = 'vito'
        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()

        config = json.loads(harvest_job.source.config)

        auth = (self.source_config['username'],
                self.source_config['password'])

        timeout = self.source_config.get('timeout', 10)
        self.update_all = self.source_config.get('update_all', False)

        collection = self.source_config['collection']

        last_product_date = (
            self._get_last_harvesting_date(harvest_job.source_id)
        )
        if last_product_date is not None:
            start_date = last_product_date
            end_date = start_date + timedelta(days=1)
        else:
            start_date, end_date = self._get_dates_from_config(config)

        ids = []

        harvest_url = self._generate_harvest_url(collection,
                                                 start_date, end_date)
        log.info('Harvesting {}'.format(harvest_url))
        if ('L2A' in collection) or ('P_V001' in collection):
            for harvest_object in self._gather_L2A_L1C(harvest_url,
                                                      timeout=timeout):
                _id = self._gather_entry(harvest_object)
                if _id:
                    ids.append(_id)
        else:
            for harvest_object in self._gather_L3(harvest_url, auth=auth,
                                                 timeout=timeout):
                _id = self._gather_entry(harvest_object)
                if _id:
                    ids.append(_id)

        return ids

    def _get_last_harvesting_date(self, source_id):
        objects = self._get_imported_harvest_objects_by_source(source_id)
        sorted_objects = objects.order_by(desc(HarvestObject.import_finished))
        last_object = sorted_objects.limit(1).first()
        if last_object is not None:
            soup = BeautifulSoup(last_object.content)
            restart_date = soup.find('dc:date').string.split('/')[1].split('T')[0]  # noqa: E501
            return datetime.strptime(restart_date, '%Y-%m-%d')
        else:
            return None

    def _get_imported_harvest_objects_by_source(self, source_id):
        return Session.query(HarvestObject).filter(
            HarvestObject.harvest_source_id == source_id,
            HarvestObject.import_finished is not None)

    def _generate_harvest_url(self, collection, start_date, end_date):
        date_format = '%Y-%m-%d'
        return URL_TEMPLATE.format(collection,
                                   start_date.strftime(date_format),
                                   end_date.strftime(date_format))

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True

    def _parse_content(self, content_str):
        content_json = json.loads(content_str)
        opensearch_contnet = content_json['content']
        content = BeautifulSoup(opensearch_contnet, 'lxml-xml')
        identifier = self._parse_identifier_element(content)
        collection = self._parse_collection_from_identifier(identifier)

        parsed_content = {}
        parsed_content['collection_name'] = collection.get_name()
        parsed_content['collection_description'] = collection.get_description()
        parsed_content['title'] = collection.get_name()
        parsed_content['tags'] = self._create_ckan_tags(collection.get_tags())  # noqa: E501
        parsed_content['uuid'] = str(uuid.uuid4())
        parsed_content['timerange_start'], parsed_content[
            'timerange_end'] = self._parse_interval(content)
        parsed_content['collection_id'] = str(collection)
        parsed_content['notes'] = parsed_content['collection_description']
        if collection.product_type == ProductType.L2A or \
                collection.product_type == ProductType.L1C:
            self._parse_L2A_L1C_content(parsed_content, identifier, content)
        else:
            extras = content_json['extras']
            file_name = extras['file_name']
            file_url = extras['file_url']
            self._parse_S_content(parsed_content, content, file_name, file_url)  # noqa: E501
        return parsed_content

    def _parse_L2A_L1C_content(self, parsed_content, identifier, content):
        parsed_content['identifier'] = self._parse_identifier(identifier)
        parsed_content['name'] = self._parse_name(identifier)
        parsed_content['spatial'] = json.dumps(
            self._bbox_to_geojson(self._parse_bbox(content)))
        metadata_url = self._get_metadata_url(content)
        product_url = self._get_product_url(content)
        thumbnail_url = self._get_thumbnail_url(content)  # noqa: E501
        parsed_content['resource'] = self._build_resources(metadata_url,
                                                           product_url,
                                                           thumbnail_url)

    def _parse_S_content(self, parsed_content, content, file_name, file_url):
        name = file_name
        parsed_content['identifier'] = self._parse_S_identifier(name)
        parsed_content['name'] = self._parse_S_name(name)
        bbox = self._generate_bbox(self._parse_coordinates(name))
        parsed_content['spatial'] = json.dumps(self._bbox_to_geojson(bbox))
        metadata_url = self._get_metadata_url(content)
        base_thumbnail_url = self._get_thumbnail_url(content)
        thumbnail_url = self._generate_tile_thumbnail_url(base_thumbnail_url,
                                                          bbox)
        parsed_content['resource'] = self._build_resources(metadata_url,
                                                           file_url,
                                                           thumbnail_url)
                

    def _generate_tile_thumbnail_url(self, thumbnail_url, bbox):
        url_parts = urlparse(thumbnail_url)
        query_params_tuple = parse_qsl(url_parts.query)
        query_params = dict(query_params_tuple)
        query_params['BBOX'] = ','.join(str(n) for n in bbox)
        query_params['HEIGHT'] = 200
        query_params['WIDTH'] = 200
        url_parts_list = list(url_parts)

        url_parts_list[4] = urlencode(
            tuple((key, query_params[key]) for key, _ in query_params_tuple))

        return unquote(urlunparse(tuple(url_parts_list)))

    def _parse_file_name(self, file_entry):
        return str(file_entry['name'])

    def _parse_S_identifier(self, name):
        return path.splitext(name)[0]

    def _parse_S_name(self, name):
        return path.splitext(name)[0].lower()

    COORDINATES_REGEX = re.compile(r'X(\d\d)Y(\d\d)')

    def _parse_coordinates(self, name):
        match = re.search(self.COORDINATES_REGEX, name)
        return int(match.group(1)), int(match.group(2))

    def _generate_bbox(self, coordinates):
        x, y = coordinates
        lng_min = -180 + 10 * x
        lng_max = lng_min + 10
        lat_max = 75 - 10 * y
        lat_min = lat_max - 10
        return [lat_min, lng_min, lat_max, lng_max]

    def _parse_file_url(self, file_entry):
        return str(file_entry.resources.url.string)

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
        return [[lng_min, lat_max], [lng_max, lat_max], [lng_max, lat_min],
                [lng_min, lat_min], [lng_min, lat_max]]

    def _parse_bbox(self, entry):
        bbox_str = entry.box.string
        bbox_parts = bbox_str.split()
        return [float(coord) for coord in bbox_parts]

    def _parse_collection_from_identifier(self, identifier):
        collection_name = identifier.split(':')[5]
        if '_P_' in collection_name:
            _, product_type, _ = collection_name.split('_')
        else:
            _, product_type, resolution_str, _ = collection_name.split('_')
            resolution = self._parse_resolution(resolution_str)
        if product_type == 'L2A':
            return L2AProbaVCollection(ProductType.L2A, resolution)
        elif product_type == 'P':
            return L1CProbaVCollection(ProductType.L1C)
        else:
            product_parts = product_type.split('-')
            frequency = int(product_parts[0][1:])
            subtype = ProductType(product_parts[1])
            ndvi = len(product_parts) > 2 and product_parts[2] == 'NDVI'
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

    def _build_resources(self, metadata_url, product_url, thumbnail_url):
        return [{
            'name': 'Metadata Download',
            'url': metadata_url,
            'format': 'xml',
            'mimetype': 'application/xml'
        }, {
            'name': 'Product Download',
            'url': product_url,
            'format': 'hdf5',
            'mimetype': 'application/x-hdf5'
        }, {
            'name': 'Thumbnail Download',
            'url': thumbnail_url,
            'format': 'png',
            'mimetype': 'image/png'
        }]

    def _get_resources(self, parsed_content):
        return parsed_content['resource']

    def _get_metadata_url(self, content):
        return str(content.find('link', title='HMA')['href'])

    def _get_product_url(self, content):
        return str(content.find('link', rel='enclosure')['href'])

    def _get_thumbnail_url(self, content):
        return str(content.find('link', rel='icon')['href'])

    def _get_url(self, url, auth=None, **kwargs):
        log.info('getting %s', url)
        if auth:
            kwargs['auth'] = HTTPBasicAuth(*auth)
        response = requests.get(url, **kwargs)
        response.raise_for_status()
        return response

    def _get_xml_from_url(self, url, auth=None, **kwargs):
        response = self._get_url(url, auth=auth, **kwargs)
        return BeautifulSoup(response.text, 'lxml-xml')

    def _gather_L2A_L1C(self, open_search_url, auth=None, timeout=10):
        for open_search_page in self._open_search_pages_from(
                open_search_url, auth=auth, timeout=timeout):
            for open_search_entry in self._parse_open_search_entries(
                    open_search_page):
                guid = self._parse_identifier_element(open_search_entry)
                restart_date = self._parse_restart_date(open_search_entry)
                content = open_search_entry.encode()
                yield self._create_harvest_object(guid, restart_date, content)  # noqa: E501

    def _gather_L3(self, open_search_url, auth=None, timeout=10):
        for open_search_page in self._open_search_pages_from(
                open_search_url, auth=auth, timeout=timeout):
            for open_search_entry in self._parse_open_search_entries(
                    open_search_page):
                metalink_url = self._parse_metalink_url(open_search_entry)
                metalink_xml = self._get_xml_from_url(metalink_url, auth)
                for metalink_file_entry in self._get_metalink_file_elements(
                        metalink_xml):
                    identifier = self._parse_identifier_element(
                        open_search_entry)
                    file_name = self._parse_file_name(metalink_file_entry)
                    guid = self._generate_L3_guid(identifier, file_name)
                    restart_date = self._parse_restart_date(open_search_entry)  # noqa: E501
                    content = open_search_entry.encode()
                    extras = {
                        'file_name': file_name,
                        'file_url': self._parse_file_url(metalink_file_entry)
                    }
                    yield self._create_harvest_object(
                        guid, restart_date, content, extras=extras)

    def _create_harvest_object(self, guid, restart_date, content, extras={}):
        return {
            'identifier': self._parse_name(guid),
            'guid': guid,
            'restart_date': restart_date,
            'content': json.dumps({
                'content': content,
                'extras': extras
            }),
        }

    def _parse_restart_date(self, open_search_entry):
        return open_search_entry.find('updated').string

    def _generate_L3_guid(self, identifier, file_name):
        return '{}:{}'.format(identifier, file_name)

    # lxml was used befor instead of lxml-xml
    def _open_search_pages_from(self,
                                harvest_url,
                                limit=100,
                                timeout=10,
                                auth=None,
                                provider=None,
                                parser='lxml-xml'):  # noqa: E501
        """
        Iterate through the results, create harvest objects,
        and return the ids.
        """
        retrieved_entries = 0
        while retrieved_entries < limit and harvest_url:
            # We'll limit ourselves to one request per second
            start_request = time.time()

            # Make a request to the website
            timestamp = str(datetime.utcnow())
            log_message = '{:<12} | {} | {} | {}s'
            try:
                kwargs = {'verify': False, 'timeout': timeout}
                r = self._get_url(harvest_url, auth=auth, **kwargs)
            except Timeout as e:
                self._save_gather_error('Request timed out: {}'.format(e),
                                        self.job)  # noqa: E501
                status_code = 408
                elapsed = 9999
                if hasattr(self, 'provider_logger'):
                    self.provider_logger.info(
                        log_message.format(self.provider, timestamp,
                                           status_code, timeout))  # noqa: E128, E501
                raise StopIteration
            if r.status_code != 200:
                self._save_gather_error('{} error: {}'.format(
                    r.status_code, r.text), self.job)  # noqa: E501
                elapsed = 9999
                if hasattr(self, 'provider_logger'):
                    self.provider_logger.info(
                        log_message.format(self.provider, timestamp,
                                           r.status_code,
                                           elapsed))  # noqa: E128
                raise StopIteration

            if hasattr(self, 'provider_logger'):
                self.provider_logger.info(
                    log_message.format(
                        self.provider, timestamp, r.status_code,
                        r.elapsed.total_seconds()))  # noqa: E128, E501

            soup = BeautifulSoup(r.content, parser)  # r.text????

            retrieved_entries += self._parse_items_per_page(soup)
            # Get the URL for the next loop, or None to break the loop
            harvest_url = self._get_next_url(soup)
            log.debug('next url: %s', harvest_url)

            end_request = time.time()
            request_time = end_request - start_request
            if request_time < 1.0:
                time.sleep(1 - request_time)
            yield soup

    def _parse_items_per_page(self, open_search_page):
        return int(open_search_page.find('itemsPerPage').string)

    def _parse_open_search_entries(self, soup):
        """Extract the entries from an OpenSearch response."""
        return soup.find_all('entry')

    HDF5_FILENAME_REGEX = re.compile(r'.*\.HDF5$')

    def _get_metalink_file_elements(self, metalinks):
        return metalinks.files.find_all(
            name='file', attrs={'name': self.HDF5_FILENAME_REGEX})

    def _parse_metalink_url(self, openseach_entry):
        return openseach_entry.find(
            'link', type="application/metalink+xml")['href']

    def _create_contents_json(self, opensearch_entry,
                              metalink_file_entry=None):
        content_dict = {'opensearch_entry': opensearch_entry}
        if metalink_file_entry is not None:
            content_dict['file_entry'] = metalink_file_entry
        return json.dumps(content_dict)

    def _gather_entry(self, entry, auth=None):
        # Create a harvest object for each entry
        entry_guid = entry['guid']
        log.debug('gathering %s', entry_guid)
        entry_name = entry['identifier'].replace('v101_', '').replace('.hdf5', '')  # noqa: E501
        entry_restart_date = entry['restart_date']

        package_query = Session.query(Package)
        query_filtered = package_query.filter(Package.name == entry_name)
        package = query_filtered.first()

        if package:
            # Meaning we've previously harvested this,
            # but we may want to reharvest it now.
            previous_obj = Session.query(HarvestObject) \
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
                log.debug('{} will not be updated.'.format(entry_name))  # noqa: E501  # noqa: E501
                status = 'unchanged'

            obj = HarvestObject(guid=entry_guid,
                                job=self.job,
                                extras=[
                                    HOExtra(key='status', value=status),
                                    HOExtra(key='restart_date', value=entry_restart_date)
                                ])

            obj.content = entry['content']
            obj.package = package
            obj.save()
            return obj.id

        elif not package:
            # It's a product we haven't harvested before.
            log.debug(
                '{} has not been harvested before. Creating a new harvest object.'.  # noqa: E501
                format(entry_name))  # noqa: E501
            obj = HarvestObject(
                guid=entry_guid,
                job=self.job,
                extras=[
                    HOExtra(key='status', value='new'),
                    HOExtra(key='restart_date', value=entry_restart_date)
                ])
            obj.content = entry['content']
            obj.package = None
            obj.save()
            return obj.id
