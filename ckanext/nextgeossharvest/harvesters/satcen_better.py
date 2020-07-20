from ckan.model import Session
from ckan.model import Package
from ckan.plugins.core import implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.nextgeossharvest.collection_description.satcen_better import COLLECTION

from ckanext.nextgeossharvest.interfaces.opensearch import OPENSEARCH as INTERFACE

from sqlalchemy import desc

from requests.exceptions import Timeout

import json
import logging
import uuid

import time
from datetime import datetime

import shapely.wkt
import shapely.geometry


log = logging.getLogger(__name__)

def check_attributes(pre_attribute_field, fixed_attributes):
    for attribute in fixed_attributes:
        if attribute['attribute'] not in pre_attribute_field:
            return None
        elif pre_attribute_field[attribute['attribute']] not in attribute['value']:
            return None
    return True


def get_field(entry, relative_path, fixed_attributes=[]):
    if len(relative_path) == 1 and relative_path[0] in entry:
        return entry[relative_path[0]]
    tag = relative_path.pop(0)
    if len(relative_path) == 1:
        if tag in entry:
            pre_content = entry[tag]
            if type(pre_content) is not list:
                pre_content = [pre_content]
            for pre_attribute_field in pre_content:
                if check_attributes(pre_attribute_field, fixed_attributes):
                    return pre_attribute_field[relative_path[0]]
            return None  
        else: 
            return None 
    else:
        new_entry = entry.get(tag, None)
        if new_entry:
            field = get_field(new_entry, relative_path, fixed_attributes)
        else:
            return None

        return field

class SatcenBetterHarvester(NextGEOSSHarvester):
    '''
    A harvester for SatcenBetter products.
    '''
    implements(IHarvester)

    def info(self):
        info =  {   'name': 'satcen_better',
                    'title': 'SatcenBetter Harvester',
                    'description': 'A Harvester for SatcenBetter Products'
        }
        return info

    def validate_config(self, config):
        if not config:
            return config

        try:
            INTERFACE.validate_config(config, COLLECTION)
        except ValueError as e:
            raise e
        return config 

    def _get_config(self, harvest_job):
        return json.loads(harvest_job.source.config)
    
    def _get_imported_harvest_objects_by_source(self, source_id):   
        return Session.query(HarvestObject).filter(
            HarvestObject.harvest_source_id == source_id,
            HarvestObject.import_finished is not None)

    def _get_last_harvesting_index(self, source_id, interface):
        """
        Return the index of the last product harvested or none
        if no previous harvesting job
        """
        objects = self._get_imported_harvest_objects_by_source(source_id)
        sorted_objects = objects.order_by(desc(HarvestObject.import_finished))
        last_object = sorted_objects.limit(1).first()
        if last_object is not None:
            index = self._get_object_extra(last_object,
                                           interface.get_pagination_mechanism(),
                                           interface.get_mininum_pagination_value())
            return index
        else:
            return None

    # Required by NextGEOSS base harvester
    def gather_stage(self, harvest_job):
        self.log = logging.getLogger(__file__)
        self.log.debug('SatcenBetter Harvester gather_stage for job: %r', harvest_job)

        self.job = harvest_job
        self.source_config = self._get_config(harvest_job)
        self.update_all = self.source_config.get('update_all', False)
        interface = INTERFACE(self.source_config, COLLECTION)

        last_product_index = (
            self._get_last_harvesting_index(harvest_job.source_id, interface)
        )
        interface.update_index(last_product_index)
        interface.build_url()
        
        log.debug('URL: {}'.format(interface.current_url) ) # noqa: E501

        ids = []
        try:
            results = interface.get_results()
        except Timeout as e:
            self._save_gather_error('Request timed out: {}'.format(e), self.job)  # noqa: E501
            return ids
        if type(results) is not list:
            self._save_gather_error('{} error: {}'.format(results['status_code'], results['message']), self.job)  # noqa: E501
            return ids

        for entry in results:
            name_path = interface.get_name_path()

            name_url = get_field(entry,
                                 name_path['relative_location'].split(","),
                                 name_path['fixed_attributes'])
            entry_name = parse_name(name_url).lower()
            entry_guid = unicode(uuid.uuid4())
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
                    HOExtra(key=interface.get_pagination_mechanism(), value=interface.get_index())
                ])
            obj.content = json.dumps(entry)
            obj.package = None if status == 'new' else package
            obj.save()
            interface.increment_index()
            ids.append(obj.id)
        return ids


    def fetch_stage(self, harvest_object):
        return True

    # Required by NextGEOSS base harvester
    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        content = json.loads(content)
        interface = INTERFACE(self.source_config, COLLECTION)
        mandatory_fields = interface.get_mandatory_fields()
        parsed_content = {}

        for key, path in mandatory_fields.items():
            if 'timerange_start' in key:
                field_value = get_field(content,
                                    path['location']['relative_location'].split(","),
                                    path['location'].get('fixed_attributes', []))

                timerange_start = parse_time(field_value, path['parse_function'], 0)
                parsed_content['timerange_start'] = timerange_start
            elif 'timerange_end' in key:
                field_value = get_field(content,
                                    path['location']['relative_location'].split(","),
                                    path['location'].get('fixed_attributes', []))

                timerange_end = parse_time(field_value, path['parse_function'], 1)
                parsed_content['timerange_end'] = timerange_end
            elif 'spatial' in key:
                field_value = get_field(content,
                                    path['location']['relative_location'].split(","),
                                    path['location'].get('fixed_attributes', []))

                spatial = parse_spatial(field_value, path['parse_function'])
                parsed_content['spatial'] = spatial
            else:
                field_value = get_field(content,
                                        path['relative_location'].split(","),
                                        path.get('fixed_attributes', []))
                parsed_content[key] = field_value

        title = parsed_content.pop('title')
        parsed_content['title'] = parse_name(title)
        parsed_content['identifier'] = parse_name(title)
        parsed_content['name'] = parse_name(title).lower()

        resource_fields = interface.get_resource_fields()
        parsed_content['resource'] = _parse_resources(content, resource_fields)

        parsed_content['tags'] = []
        return parsed_content

    # Required by NextGEOSS base harvester
    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        return metadata['resource']


def parse_name(url):
    filename = url.split('/')[-1]
    name = '.'.join(filename.split('.')[:-1])
    return name

def parse_time(field_value, parsing_type, instance=0):
    if parsing_type == 'custom':
        split_title = field_value.split(' ')
        return split_title[-1] if instance else split_title[-3]
    elif parsing_type == 'complete_slash':
        if '/' in field_value:
            start, end = field_value.split('/')
        else:
            start = field_value
            end = start
            
        return end if instance else start

def parse_spatial(field_value, parsing_type):
    if parsing_type == 'GeoJSON':
        return json.dumps(field_value)
    elif parsing_type == 'WKT':
        g1 = shapely.wkt.loads(field_value)
        g2 = shapely.geometry.mapping(g1)
        return json.dumps(g2)

def _parse_resources(content, resource_fields):
    resources = []
    for resource in resource_fields:
        single_resource = {}
        for key, field in resource.items():
            if field["field_type"] == 'freeText':
                single_resource[key] = field["location"]["relative_location"]
            elif field["field_type"] == 'path':
                field_value = get_field(content,
                                        field['location']['relative_location'].split(","),
                                        field['location'].get('fixed_attributes', []))
                if field_value:
                    single_resource[key] = field_value
                else:
                    break
        resources.append(single_resource)
    return resources

