# -*- coding: utf-8 -*-

import json
import logging
from datetime import datetime
from sqlalchemy import desc
import uuid
from os import path
import mimetypes
import stringcase
import re

from ckan.model import Session
from ckan.model import Package
from ckan.plugins.core import implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.nextgeossharvest.lib.wfs import WFS
from ckanext.nextgeossharvest.lib.scent_config import COLLECTION
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

log = logging.getLogger(__name__)

def parse_file_extension(url):
    fname = url.split('/')[-1]
    return path.splitext(fname)[1]

def convert_to_clean_snakecase(extra_key):
        clean_extra_key = re.sub('[^0-9a-zA-Z]+', '_', stringcase.snakecase(extra_key)).strip('_')
        return clean_extra_key

class SCENTHarvester(NextGEOSSHarvester):
    '''
    A harvester for SCENT products.
    '''
    implements(IHarvester)

    def info(self):
        info =  {   'name': 'scent',
                    'title': 'SCENT Harvester',
                    'description': 'A Harvester for SCENT Products'
        }
        return info

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'wfs_url' not in config_obj:
                raise ValueError('The parameter wfs_url is required')

            if 'wfs_version' not in config_obj:
                raise ValueError('The parameter wfs_version is required')

            if 'collection' in config_obj:
                collection = config_obj['collection']
                if collection not in COLLECTION:
                    err_msg = '"collection" must be one of the entries of {}'
                    raise ValueError(err_msg.format(list(COLLECTION.keys())))
            else:
                raise ValueError('"collection" is required')

            if type(config_obj.get('max_dataset', 100)) != int:
                raise ValueError('max_dataset must be an integer')
            
            if type(config_obj.get('update_all', False)) != bool:
                raise ValueError('update_all must be true or false')
        except ValueError as e:
            raise e

        return config

    def _get_config(self, harvest_job):
        return json.loads(harvest_job.source.config)

    # Required by NextGEOSS base harvester
    def gather_stage(self, harvest_job):
        self.log = logging.getLogger(__file__)
        self.log.debug('SCENT Harvester gather_stage for job: %r', harvest_job)

        self.job = harvest_job
        self.source_config = self._get_config(harvest_job)
        max_dataset = self.source_config.get('max_dataset', 100)
        wfs_url = self.source_config.get('wfs_url')
        wfs_version = self.source_config.get('wfs_version')
        collection = self.source_config.get('collection')
        typename = COLLECTION[collection].get('collection_typename')
        tag_typename = COLLECTION[collection].get('tag_typename', None)
        self.update_all =  self.source_config.get('update_all', False)

        last_product_index = (
            self._get_last_harvesting_index(harvest_job.source_id)
        )

        if last_product_index is not None:
            last_product_index =+ 1
        else:
            last_product_index = 0

        wfs = WFS(url=wfs_url, version=wfs_version)

        wfs.set_collection(typename)
        sortby=['When']

        result = wfs.make_request(max_dataset, sortby, last_product_index)
        entries = result['features']
        name = '{}_{}'.format(collection.lower(), '{}')
        ids = []
        for entry in entries:
            entry_guid = unicode(uuid.uuid4())
            entry_name = name.format(entry['id'])
            log.debug('gathering %s', entry_name)

            
            content = {}
            content['collection_content'] = entry
            if tag_typename:
                wfs.set_collection(tag_typename)
                filterxml = wfs.set_filter_equal_to('image_id', entry['id'])
                result = wfs.make_request(constraint=filterxml)
                result = wfs.get_request(constraint=filterxml)
                content['tag_url'] = result

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
                    log.debug('{} already exists and will be updated.'.format(
                        entry_name))  # noqa: E501
                    status = 'change'

                else:
                    log.debug(
                        '{} will not be updated.'.format(entry_name))  # noqa: E501
                    status = 'unchanged'

            elif not package:
                # It's a product we haven't harvested before.
                log.debug(
                    '{} has not been harvested before. Creating a new harvest object.'.  # noqa: E501
                    format(entry_name))  # noqa: E501
                status = 'new'
            obj = HarvestObject(
                guid=entry_guid,
                job=self.job,
                extras=[
                    HOExtra(key='status', value=status),
                    HOExtra(key='index', value=last_product_index)
                ])
            obj.content = json.dumps(content)
            obj.package = None if status == 'new' else package
            obj.save()
            last_product_index += 1
            ids.append(obj.id)
        return ids

    def fetch_stage(self, harvest_object):
        return True

    def _get_imported_harvest_objects_by_source(self, source_id):   
        return Session.query(HarvestObject).filter(
            HarvestObject.harvest_source_id == source_id,
            HarvestObject.import_finished is not None)

    def _get_last_harvesting_index(self, source_id):
        """
        Return the index of the last product harvested or none
        if no previous harvesting job
        """
        objects = self._get_imported_harvest_objects_by_source(source_id)
        sorted_objects = objects.order_by(desc(HarvestObject.import_finished))
        last_object = sorted_objects.limit(1).first()
        if last_object is not None:
            index = self._get_object_extra(last_object,'index', '1')
            return int(index)
        else:
            return None

    # Required by NextGEOSS base harvester
    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        content = json.loads(content)
        collection_content = content['collection_content']
        tag_url = content.get('tag_url', None)
        collection = self.source_config.get('collection')

        item = {}
        properties = collection_content['properties']
        item = self._parse_properties(properties, item, collection)
        resource_url = self._get_main_resource(properties, collection)
        when_date = item.pop('When')
        item['timerange_start'] = when_date
        item['timerange_end'] = when_date
        item['spatial'] = json.dumps(collection_content['geometry'])

        item = self._add_collection(item, collection)
    
        id_number = collection_content['id']
        identifier = '{}_{}'.format(collection.lower(), id_number)
        item['identifier'] = identifier
        item['name'] = identifier.lower()

        item['title'] = item['collection_name']
        item['notes'] = item['collection_description']

        item['tags'] = self._get_tags_for_dataset()
        tag_url = content.get('tag_url', None)
        item['resource'] = self._parse_resources(resource_url, tag_url)

        parsed_content = {}
        for key in item:
            new_key = convert_to_clean_snakecase(key)
            parsed_content[new_key] = item[key]

        return parsed_content

    def _parse_properties(self, properties, parsed_dict, collection):
        for key in properties:
            if key not in COLLECTION[collection].get('property_ignore_list', None):
                parsed_dict[key] = properties[key]
        return parsed_dict

    def _get_main_resource(self,properties, collection):
        url_key = COLLECTION[collection].get('url_key', None)
        url_value = properties.get(url_key, None)
        return url_value

    def _add_collection(self, item, collection):

        name = COLLECTION[collection].get('collection_name')
        description = COLLECTION[collection].get('collection_description')

        item['collection_id'] = collection
        item['collection_name'] = name
        item['collection_description'] = description
        return item

    def _get_tags_for_dataset(self):
        tags = [{'name': 'Scent'}]
        return tags

    def _make_resource(self, url, name, description, extension, file_mimetype=None):
            """
            Create the resource dictionary.
            """
            
            resource = {
                "name": name,
                "description": description,
                "url": url,
                "format": extension
            }
            if file_mimetype:
                resource["mimetype"] = file_mimetype

            return resource

    def _parse_resources(self, main_url, tag_url=None):
        resources = []

        extension = parse_file_extension(main_url)
        file_mimetype = mimetypes.types_map[extension]
        extension = extension.strip('.').upper()
        title = "Product Download"
        description = "URI for accessing the {} file.".format(file_mimetype.split('/')[0])
        resources.append(self._make_resource(main_url, title, description, extension, file_mimetype))

        if tag_url:
            if 'query' in tag_url:
                tag_url = tag_url.replace('query', 'filter')
            extension = ".json"
            file_mimetype = mimetypes.types_map[extension]
            extension = extension.strip('.').upper()
            title = "Image tags"
            description = "URI for accessing the {} file containing the different tags information.".format(file_mimetype.split('/')[0])
            resources.append(self._make_resource(tag_url, title, description, extension, file_mimetype))
        
        return resources

    # Required by NextGEOSS base harvester
    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        return metadata['resource']