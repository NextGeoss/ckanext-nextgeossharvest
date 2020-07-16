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


log = logging.getLogger(__name__)

def check_attributes(pre_attribute_field, fixed_attributes):
    for attribute in fixed_attributes:
        if attribute['attribute'] not in pre_attribute_field:
            return None
        elif pre_attribute_field[attribute['attribute']] not in attribute['value']:
            return None
    return True


def get_field(entry, relative_path, fixed_attributes):
    tag = relative_path.pop(0)
    if len(relative_path) == 1:
        pre_content = entry[tag]
        if type(pre_content) is list:
            pre_content = [pre_content]
        for pre_attribute_field in pre_content:
            if check_attributes(pre_attribute_field, fixed_attributes):
                return pre_attribute_field[relative_path[0]]
        return None
           
    else:
        field = get_field(entry[tag], relative_path, fixed_attributes)
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
        source_config = self._get_config(harvest_job)
        self.update_all = source_config.get('update_all', False)
        interface = INTERFACE(source_config, COLLECTION)

        last_product_index = (
            self._get_last_harvesting_index(harvest_job.source_id, interface)
        )

        interface.update_index(last_product_index)

        ids = []
        try:
            results = interface.get_results()
        except Timeout as e:
            self._save_gather_error('Request timed out: {}'.format(e), self.job)  # noqa: E501
            return ids
        if type(results) is not list:
            self._save_gather_error('{} error: {}'.format(results.status_code, results.text), self.job)  # noqa: E501
            return ids

        for entry in results:
            name_path = interface.get_name_path()
            name_url = get_field(entry,
                                    name_path['relative_location'].split(","),
                                    name_path['fixed_attributes'])
            entry_name = self.parse_name(name_url).lower()
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
                    HOExtra(key='index', value=last_product_index)
                ])
            obj.content = json.dumps(entry)
            obj.package = None if status == 'new' else package
            obj.save()
            last_product_index += 1
            ids.append(obj.id)
        return ids

    def parse_name(self, url):
        filename = url.split('/')[-1]
        name = '.'.join(filename.split('.')[:-1])
        return name

    def fetch_stage(self, harvest_object):
        return True

    # Required by NextGEOSS base harvester
    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
