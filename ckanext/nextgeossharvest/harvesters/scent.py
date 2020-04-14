# -*- coding: utf-8 -*-

import json
import logging
from datetime import datetime
from sqlalchemy import desc
import uuid

from ckan.model import Session
from ckan.plugins.core import implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.nextgeossharvest.lib.wfs import WFS
from ckanext.nextgeossharvest.lib.scent_config import COLLECTION
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

log = logging.getLogger(__name__)

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

    # Required by NextGEOSS base harvester
    def gather_stage(self, harvest_job):
        self.log = logging.getLogger(__file__)
        self.log.debug('SCENT Harvester gather_stage for job: %r', harvest_job)

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
        if tag_typename and wfs.set_collection(tag_typename):
            tag_schema = wfs.get_schema()

        wfs.set_collection(typename)
        collection_schema = wfs.get_schema()
        sortby=['When']
        result = wfs.make_request(max_dataset, sortby, last_product_index)
        entries = result['features']
        name = '{}_{}'.format(collection.lower(), '{}')
        ids = []
        for entry in entries:
            entry_guid = unicode(uuid.uuid4())
            entry_name = name.format(entry['id'])
            log.debug('gathering %s', entry_name)

            wfs.set_collection(tag_typename)
            filterxml = wfs.set_filter_equal_to('image_id', entry['id'])
            result = wfs.make_request(constraint=filterxml)
            tag_list = result['features']
            content = {}
            content['collection_schema'] = collection_schema
            content['collection_content'] = entry
            if tag_list:
                content['tag_schema'] = tag_schema
                content['tag_content'] = tag_list

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
            obj.package = None
            obj.save()
            last_product_index += 1
        return ids

    def fetch_stage(self, harvest_object):
        return True

    def _get_last_harvesting_index(self, source_id):
        """
        Return the index of the last product harvested or none
        if no previous harvesting job
        """
        objects = self._get_imported_harvest_objects_by_source(source_id)
        sorted_objects = objects.order_by(desc(HarvestObject.import_finished))
        last_object = sorted_objects.limit(1).first()
        if last_object is not None:
            index = json.loads(last_object.content)['index']
            return int(index)
        else:
            return None