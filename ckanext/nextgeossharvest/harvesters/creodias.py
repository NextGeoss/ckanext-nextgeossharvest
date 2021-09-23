from ckan.model import Session
from ckan.model import Package
from ckan.plugins.core import implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester
from ckanext.nextgeossharvest.lib.aux_harvest import AuxHarvester

from ckanext.nextgeossharvest.collection_description.vito_collection import COLLECTION
#from ckanext.nextgeossharvest.interfaces.opensearch import OPENSEARCH as INTERFACE

from sqlalchemy import desc

from requests.exceptions import Timeout

import json
import logging
import uuid

log = logging.getLogger(__name__)


class CREODIASHarvester(NextGEOSSHarvester, AuxHarvester):
    '''
    A harvester for CREODIAS products.
    '''
    implements(IHarvester)

    def info(self):
        info =  {   'name': 'CREODIAS',
                    'title': 'CREODIAS Harvester',
                    'description': 'A Harvester for CREODIAS Products'
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
                                           interface.get_minimum_pagination_value())
            return index
        else:
            return None

    # Required by NextGEOSS base harvester
    def gather_stage(self, harvest_job):
        self.log = logging.getLogger(__file__)
        self.log.debug('CREODIAS Harvester gather_stage for job: %r', harvest_job)
        
        self.job = harvest_job
        self.source_config = self._get_config(harvest_job)
        self.update_all = self.source_config.get('update_all', False)
        interface = INTERFACE(self.source_config, COLLECTION)
        last_product_index = (
            self._get_last_harvesting_index(harvest_job.source_id, interface)
        )
        interface.update_index(last_product_index)
        interface.build_url_date()
        
        path_to_entries = interface.get_entries_path()

        ids = []
        try:
            results = interface.get_results()
            if results:
                entries = self.get_field(results, path_to_entries[:])
            else:
                return ids
        except Timeout as e:
            self._save_gather_error('Request timed out: {}'.format(e), self.job)  # noqa: E501
            return ids
        except Exception as e:
            return ids
        if entries == None:
            return ids
        elif type(entries) is not list:
            entries = [entries]

        identifier_path = interface.get_identifier_path()

        for entry in entries:
            entry_id = self.clean_snakecase(self.get_field(entry, identifier_path[:])[0])
            entry_guid = unicode(uuid.uuid4())
            package_query = Session.query(Package)
            query_filtered = package_query.filter(Package.name == entry_id)
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
                        entry_id))  # noqa: E501
                    status = 'change'

                else:
                    log.debug(
                        '{} will not be updated.'.format(entry_id))  # noqa: E501
                    status = 'unchanged'

            elif not package:
                # It's a product we haven't harvested before.
                log.debug(
                    '{} has not been harvested before. Creating a new harvest object.'.  # noqa: E501
                    format(entry_id))  # noqa: E501
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
        parsed_content.update(interface.get_collection_info())
        for key, descriptor in mandatory_fields.items():
            field_value = []
            if "spatial" == key:
                for spatial_field in descriptor["path"]:
                    field_value.extend(self.get_field(content, spatial_field[:]))
                field_value = self.spatial_parsing(field_value,
                                                   descriptor["parsing_function"])
            else:
                field_value = self.get_field(content, descriptor["path"][:])

                if "timerange_start" in key:
                    field_value = self.temporal_parsing(field_value[0],
                                                        descriptor["parsing_function"],
                                                        0)
                elif "timerange_start" in key:
                    field_value = self.temporal_parsing(field_value[0],
                                                        descriptor["parsing_function"],
                                                        1)
                elif "tags" in key:
                    field_value = self.tag_parsing(field_value)
                else:
                    field_value = field_value[0]
            
            if field_value:
                parsed_content[key] = field_value
        
        parsed_content["name"] = self.clean_snakecase(parsed_content["identifier"])

        if "notes" not in parsed_content:
            parsed_content["notes"] = parsed_content["collection_description"]

        if "tags" not in parsed_content:
            parsed_content["tags"] = []

        resource_fields = interface.get_resource_fields()
        parsed_content['resource'] = self._parse_resources(content, resource_fields)

        extra_fields = interface.get_extras_fields()
        parsed_content.update(self._parse_extras(content, extra_fields))

        return parsed_content

    # Required by NextGEOSS base harvester
    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        return metadata['resource']


import json
import xmltodict
import requests
import datetime
from requests.auth import HTTPBasicAuth

class INTERFACE():

    def __init__(self, config, COLLECTION):
        
        self.start_index = self.get_minimum_pagination_value()
        
        self.timeout = config.get('timeout', 10)
        self.username = config.get('username', None)
        self.password = config.get('password', None)
        self.collection = config.get('collection', 'Sentinel1')
        self.current_url = 'https://finder.creodias.eu/resto/api/collections/Sentinel1/search.json?productType=SLC&startDate=2021-01-01T23:59:59'
        
        self.start_date=config.get('start_date','2021-01-01T00:00:00')
        self._collection_id = config.get('collection')
        self._collection = COLLECTION[self._collection_id]
        
        self.build_url()
    
    def get_pagination_mechanism(self):
        return self.page_start_keyword

    def get_minimum_pagination_value(self):
        return 1

    @staticmethod
    def validate_config(config):
        config_obj = json.loads(config)

        if 'collection' not in config_obj:
            raise ValueError('The parameter collection is required(Sentinel1, Sentinel2, Sentinel5p)')

        if 'start_date' not in config_obj:
            raise ValueError('The parameter start_date is required')
        

        if type(config_obj.get('max_dataset', 100)) != int:
            raise ValueError('max_dataset must be an integer')
        
        if type(config_obj.get('update_all', False)) != bool:
            raise ValueError('update_all must be true or false')
        
        if type(config_obj.get('timeout', 10)) != int:
            raise ValueError('timeout must be an integer')

    def build_url(self):
        if '?' in self.current_url:
            base_url, query = self.current_url.split('?')
        else:
            base_url = self.current_url
            query = ""

        if 'startDate' not in query:
            query += '&{}={}'.format('startDate', str(self.start_date))
        if 'index' not in query:
            query += '&{}={}'.format('index', str(self.start_index))
    
        query_components = query.split('&')
        for i, component in enumerate(query_components):
            if component.startswith('startDate'):
                query_components[i] = '{}={}'.format('startDate', str(self.start_date))
            elif component.startswith('index'):
                query_components[i] = '{}={}'.format('index', str(self.start_index))
            

        query = '&'.join(query_components)
        self.current_url = '{}?{}'.format(base_url, query)
    
    def update_index(self, index=None):
        # possible hook for non integer pagination mechanisms
        min_index = self.get_minimum_pagination_value()
        self.start_index = int(index) if index else min_index

    def increment_index(self):
        # possible hook for non integer pagination mechanisms
        self.start_index += 1

    def get_index(self):
        return self.start_index

    def get_results(self):
        if self.username and self.password:
            req = requests.get(self.current_url,
                            auth=HTTPBasicAuth(self.username, self.password),
                            verify=False, timeout=self.timeout)
        else:
            req = requests.get(self.current_url,
                            verify=False, timeout=self.timeout)

        if req.status_code != 200:
            return None
        else:
            content_type = req.headers['content-type']
            if 'json' in content_type:
                results = json.loads(req.text)
            elif 'xml' in content_type:
                results = xmltodict.parse(req.text)
            else:
                return None
            return results

    def get_entries_path(self):
        return self._collection["dataset_tag"]["path"]

    def get_identifier_path(self):
        mandatory_fields = self.get_mandatory_fields()
        return mandatory_fields["identifier"]["path"]

    def get_mandatory_fields(self):
        return self._collection["mandatory_fields"]

    def get_resource_fields(self):
        return self._collection["resources"]

    def get_extras_fields(self):
        return self._collection["extras"]

    def get_collection_info(self):
        collection_info = {
            "collection_id": self._collection_id,
            "collection_name": self._collection["collection_name"],
            "collection_description": self._collection["collection_description"]
        }
        return collection_info
