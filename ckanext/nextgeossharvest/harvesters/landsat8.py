# -*- coding: utf-8 -*-
import re
from enum import Enum
import logging
import json
import uuid
from datetime import datetime
from sqlalchemy import desc

import os

from boto3.session import Session as boto3_session
import itertools
from functools import partial
from concurrent import futures

from ckanext.nextgeossharvest.lib import aws

from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckan.model import Package

from ckanext.harvest.model import HarvestObjectExtra as HOExtra

from landsat8_collections import COLLECTION_DESCRIPTIONS

log = logging.getLogger(__name__)

AWS_REGION = 'us-east-1'
MAX_WORKER = 50
ROW_MAX = 233
PATH_MAX = 248

log = logging.getLogger(__name__)


def parse_date(date_str, scenario=None):
    date = datetime.strptime(date_str, "%Y%m%d")
    if scenario == "start":
        return date.strftime("%Y-%m-%dT00:00:00.000Z")
    elif scenario == "end":
        return date.strftime("%Y-%m-%dT23:59:59.999Z")
    else:
        return date.strftime("%Y-%m-%d")



"""Errors and warnings."""


class SatApiError(Exception):
    """Base exception class."""


class InvalidLandsatSceneId(SatApiError):
    """Invalid Landsat-8 scene id."""


class ProductType(Enum):
    T1 = 'T1'
    RT = 'RT'
    T2 = 'T2'


class Landsat8Collection(object):
    def __init__(self, product_type):
        self.product_type = product_type

    def get_description(self):
        return COLLECTION_DESCRIPTIONS[self.get_name()]

    def get_tags(self):
        return ['Landsat-8', 'L8-{}'.format(self.product_type)]

    def __str__(self):
        return 'Landsat_8_{}'.format(self.product_type)

    def get_name(self):
        return 'Landsat-8 {}'.format(self.product_type)


class Landsat8Harvester(NextGEOSSHarvester):
    """
    A an example of how to build a harvester for OpenSearch sources.
    You'll want to add some custom code (or preferably a custom class) to
    handle parsing the entries themselves, as well as any special logic
    for deciding which entries to import, etc.
    """
    implements(IHarvester)

    def info(self):
        return {
            'name': 'landsat8',
            'title': 'Landsat-8 Harvester',
            'description': 'A Harvester for Landsat-8 Products'
        }

    def validate_config(self, config):
        if not config:
            return config
        try:
            config_obj = json.loads(config)

            if 'path' in config_obj:
                path = config_obj['path']
                if not isinstance(path, int) and not path > 0:
                    raise ValueError('path must be a positive integer')

            if 'row' in config_obj:
                row = config_obj['row']
                if not isinstance(row, int) and not row > 0:
                    raise ValueError('row must be a positive integer')

            if type(config_obj.get('access_key', None)) != unicode:
                raise ValueError('AWS access_key is required and must be'
                                 ' a string')

            if type(config_obj.get('secret_key', None)) != unicode:
                raise ValueError('AWS secret_key is required and must be'
                                 ' a string')

            if type(config_obj.get('bucket', None)) != unicode:
                raise ValueError('AWS-S3 bucket is required and must be'
                                 ' a string')

            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')
        except ValueError as e:
            raise e

        return config

    def _set_s3_session(self):

        access_key = self.source_config['access_key']
        secret_key = self.source_config['secret_key']

        session = boto3_session(region_name=AWS_REGION)
        s3 = session.client('s3',
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key)

        return s3

    # TODO: define self.provider in logs
    def gather_stage(self, harvest_job):
        self.job = harvest_job
        self._set_source_config(self.job.source.config)
        log.debug('Landsat-8 Harvester gather_stage for job: %r', harvest_job)

        self.provider = 'usgs'
        if not hasattr(self, 'provider_logger'):
            self.provider_logger = self.make_provider_logger()

        last_path_row = self._get_last_harvesting_tile(harvest_job.source_id)
        if last_path_row is not None:
            path = int(last_path_row[0])
            row = int(last_path_row[1])

            if path + 1 > PATH_MAX:
                path = 1
                row = 1 if row + 1 > ROW_MAX else row + 1
            else:
                path = path + 1
        else:
            path = self.source_config.get('path', 1)
            row = self.source_config.get('row', 1)

        bucket = self.source_config['bucket']
        s3 = self._set_s3_session()
        _ls_worker = partial(aws.list_directory, bucket, s3=s3)

        ids = []

        log.info('Harvesting Path: {} and Row: {}'.format(path, row))
        path = self._zeropad(path, 3)
        row = self._zeropad(row, 3)

        prefixes = ['c1/L8/{}/{}/'.format(path, row)]

        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            results = executor.map(_ls_worker, prefixes)
            results = itertools.chain.from_iterable(results)

        scene_ids = [os.path.basename(key.strip('/')) for key in results]

        while not scene_ids:
            path = int(path)
            row = int(row)

            if path + 1 > PATH_MAX:
                path = 1
                row = 1 if row + 1 > ROW_MAX else row + 1
            else:
                path = path + 1

            log.info('Harvesting Path: {} and Row: {}'.format(path, row))
            path = self._zeropad(path, 3)
            row = self._zeropad(row, 3)

            prefixes = ['c1/L8/{}/{}/'.format(path, row)]

            with futures.ThreadPoolExecutor(max_workers=2) as executor:
                results = executor.map(_ls_worker, prefixes)
                results = itertools.chain.from_iterable(results)

            scene_ids = [os.path.basename(key.strip('/')) for key in results]

        for scene in scene_ids:
            _id = self._gather_entry(scene, int(path), int(row))
            if _id:
                ids.append(_id)

        return ids

    def _get_last_harvesting_tile(self, source_id):
        objects = self._get_imported_harvest_objects_by_source(source_id)
        sorted_objects = objects.order_by(desc(HarvestObject.import_finished))
        last_object = sorted_objects.limit(1).first()
        if last_object is not None:
            path = self._get_object_extra(last_object,
                                          'path', '1')
            row = self._get_object_extra(last_object,
                                         'row', '1')

            return (path, row)
        else:
            return None

    def _get_imported_harvest_objects_by_source(self, source_id):
        return Session.query(HarvestObject).filter(
            HarvestObject.harvest_source_id == source_id,
            HarvestObject.import_finished is not None)

    def _zeropad(self, n, l):
        """ Add leading 0."""
        return str(n).zfill(l)

    def _landsat_parse_scene_id(self, sceneid):
        """Parse Landsat-8 scene id.
        """
        collection_1 = r'(L[COTEM]08_L\d{1}[A-Z]{2}_\d{6}_\d{8}_\d{8}_\d{2}_(T1|T2|RT))'  # noqa: E501
        if not re.match('^{}$'.format(collection_1), sceneid):
            raise InvalidLandsatSceneId('Could not match {}'.format(sceneid))

        collection_pattern = (
            r'^L'
            r'(?P<sensor>\w{1})'
            r'(?P<satellite>\w{2})'
            r'_'
            r'(?P<correction_level>\w{4})'
            r'_'
            r'(?P<path>[0-9]{3})'
            r'(?P<row>[0-9]{3})'
            r'_'
            r'(?P<acquisition_date>[0-9]{4}[0-9]{2}[0-9]{2})'
            r'_'
            r'(?P<ingestion_date>[0-9]{4}[0-9]{2}[0-9]{2})'
            r'_'
            r'(?P<collection>\w{2})'
            r'_'
            r'(?P<category>\w{2})$')

        meta = None
        for pattern in [collection_pattern]:
            match = re.match(pattern, sceneid, re.IGNORECASE)
            if match:
                meta = match.groupdict()
                break

        collection = meta.get('collection', '')
        if collection != '':
            collection = 'c{}'.format(int(collection))

        meta['scene_id'] = sceneid
        meta['satellite'] = 'L{}'.format(meta['satellite'].lstrip('0'))
        meta['key'] = os.path.join(collection, 'L8', meta['path'],
                                   meta['row'], sceneid, sceneid)

        return meta

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True

    def _parse_content(self, content_str):
        scene_id = content_str

        s3 = self._set_s3_session()
        parsed_content = self.get_l8_info(scene_id, True, s3)

        identifier = scene_id.lower()
        collection = Landsat8Collection(parsed_content['category'])

        parsed_content['collection_name'] = collection.get_name()
        parsed_content['collection_description'] = collection.get_description()
        parsed_content['collection_id'] = str(collection)
        parsed_content['title'] = collection.get_name()
        parsed_content['tags'] = self._create_ckan_tags(collection.get_tags())

        acquisition_date = parsed_content.pop('acquisition_date')
        parsed_content['timerange_start'] = parse_date(acquisition_date, "start")
        parsed_content['timerange_end'] = parse_date(acquisition_date, "end")

        ingestion_date = parsed_content.get("ingestion_date", None)
        if ingestion_date:
            parsed_content["ingestion_date"] = parse_date(ingestion_date)

        parsed_content['notes'] = parsed_content['collection_description']

        parsed_content['name'] = identifier
        parsed_content['spatial'] = json.dumps(parsed_content.pop('geometry'))
        parsed_content.pop('key')
        return parsed_content

    def get_l8_info(self, scene_id, full=False, s3=None):
        """Return Landsat-8 metadata."""
        bucket = self.source_config['bucket']
        info = self._landsat_parse_scene_id(scene_id)

        aws_url = 'https://{}.s3.amazonaws.com'.format(bucket)
        scene_key = info["key"]

        bands = ['B1', 'B2', 'B3', 'B4', 'B5', 'B6',
                 'B7', 'B8', 'B9', 'B10', 'B11', 'BQA']
        info['resource'] = {}
        for band in bands:
            info['resource'][band] = self.get_l8_resources(aws_url, scene_key,
                                                           band)

        scene_url = '{}/{}'.format(aws_url, scene_key)

        info['thumbnail'] = scene_url + '_thumb_small.jpg'

        if full:
            try:
                data = json.loads(aws.get_object(bucket, scene_key +
                                                 '_MTL.json', s3=s3))
                img_attr = data['L1_METADATA_FILE']['IMAGE_ATTRIBUTES']
                prod_meta = data['L1_METADATA_FILE']['PRODUCT_METADATA']

                info['sun_azimuth'] = img_attr.get('SUN_AZIMUTH')
                info['sun_elevation'] = img_attr.get('SUN_ELEVATION')
                info['cloud_coverage'] = img_attr.get('CLOUD_COVER')
                info['cloud_coverage_land'] = img_attr.get('CLOUD_COVER_LAND')
                info['geometry'] = {
                    'type': 'Polygon',
                    'coordinates': [[
                        [prod_meta['CORNER_UR_LON_PRODUCT'],
                         prod_meta['CORNER_UR_LAT_PRODUCT']],
                        [prod_meta['CORNER_UL_LON_PRODUCT'],
                         prod_meta['CORNER_UL_LAT_PRODUCT']],
                        [prod_meta['CORNER_LL_LON_PRODUCT'],
                         prod_meta['CORNER_LL_LAT_PRODUCT']],
                        [prod_meta['CORNER_LR_LON_PRODUCT'],
                         prod_meta['CORNER_LR_LAT_PRODUCT']],
                        [prod_meta['CORNER_UR_LON_PRODUCT'],
                         prod_meta['CORNER_UR_LAT_PRODUCT']]
                    ]]}
                info['resource']['mtl'] = scene_url + '_MTL.json'
                info['resource']['ang'] = scene_url + '_ANG.txt'
            except Exception:
                print('Could not get info from {}_MTL.json'.format(scene_key))

        return info

    def get_l8_resources(self, aws_url, scene_key, band):
        resource = {}
        url = '{}/{}_{}'.format(aws_url, scene_key, band)
        resource['tif'] = url + '.TIF'
        resource['ovr'] = url + '.TIF.ovr'
        resource['imd'] = url + '_wrk.IMD'
        return resource

    def _create_ckan_tags(self, tags):
        return [{'name': tag} for tag in tags]

    def _get_resources(self, parsed_content):
        resources = parsed_content['resource']
        resource_list = []
        for key in resources:
            if key.startswith('B'):
                band_resources = self._parse_band_resource(key, resources[key])
                for band_resource in band_resources:
                    resource_list.append(band_resource)
            elif key == 'mtl':
                parsed_resource = {
                    'name': 'Download L1 Metadata file',
                    'url': resources[key],
                    'format': 'JSON',
                    'mimetype': 'application/json'}
                resource_list.append(parsed_resource)
            elif key == 'ang':
                parsed_resource = {
                    'name': 'Download Angle-coefficent file.',
                    'url': resources[key],
                    'format': 'TXT',
                    'mimetype': 'text/plain'}
                resource_list.append(parsed_resource)
        return resource_list

    def _parse_band_resource(self, key, content):

        band_resource = []
        for resource in content:
            if resource == 'tif':
                name = '{} GeoTIF Download'.format(key)
                url = content[resource]
                file_ext = 'TIF'
                mimetype = 'image/tif'
            elif resource == 'ovr':
                name = '{} Overlay Marker Download'.format(key)
                url = content[resource]
                file_ext = 'OVR'
                mimetype = 'application/octet-stream'
            elif resource == 'imd':
                name = 'Georeferencing information for {}'.format(key)
                url = content[resource]
                file_ext = 'IMD'
                mimetype = 'application/octet-stream'

            parsed_resource = {
                'name': name,
                'url': url,
                'format': file_ext,
                'mimetype': mimetype}

            band_resource.append(parsed_resource)
        return band_resource

    def _gather_entry(self, entry, path, row, update_all=False):
        # Create a harvest object for each entry
        entry_guid = unicode(uuid.uuid4())
        entry_name = entry.lower()  # noqa: E501
        log.debug('gathering %s', entry)

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

            if update_all:
                log.debug('{} already exists and will be updated.'.format(
                    entry_name))  # noqa: E501
                status = 'change'
                obj = HarvestObject(
                    guid=entry_guid,
                    job=self.job,
                    extras=[
                        HOExtra(key='status', value=status),
                        HOExtra(key='path', value=path),
                        HOExtra(key='row', value=row)
                    ])
                obj.content = entry
                obj.package = package
                obj.save()
                return obj.id

            else:
                log.debug(
                    '{} will not be updated.'.format(entry_name))  # noqa: E501
                status = 'unchanged'
                obj = HarvestObject(
                    guid=entry_guid,
                    job=self.job,
                    extras=[
                        HOExtra(key='status', value=status),
                        HOExtra(key='path', value=path),
                        HOExtra(key='row', value=row)
                    ])
                obj.content = entry
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
                    HOExtra(key='path', value=path),
                    HOExtra(key='row', value=row)
                ])
            obj.content = entry
            obj.package = None
            obj.save()
            return obj.id
