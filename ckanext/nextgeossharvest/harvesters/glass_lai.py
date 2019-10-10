# -*- coding: utf-8 -*-

import logging
import json
from ckan.plugins.core import implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.nextgeossharvest.lib.glass_lai_base import GLASS_LAI_Base
from ckanext.nextgeossharvest.lib.opensearch_base import OpenSearchHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester


class GLASS_LAI_Harvester(GLASS_LAI_Base, OpenSearchHarvester, NextGEOSSHarvester):  # noqa: E501
    """
    A Harvester for GLASS Leaf Area Index (LAI).
    """
    implements(IHarvester)

    def info(self):
        return {
            'name': 'glass_lai',
            'title': 'GLASS LAI',
            'description': 'A Harvester for GLASS Leaf Area Index (LAI).'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)
            if config_obj.get('sensor') not in {'avhrr', 'modis'}:
                raise ValueError('sensor is required and must be "avhrr" or "modis"')  # noqa: E501
            if 'make_private' in config_obj:
                if type(config_obj.get('make_private', False)) != bool:
                    raise ValueError('make_private must be true or false')
            else:
                raise ValueError('make_private must be true or false')
        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        self.log = logging.getLogger(__file__)
        self.log.debug('GLASS LAI Harvester gather_stage for job: %r', harvest_job)  # noqa: E501

        # Save a reference
        self.job = harvest_job

        self._set_source_config(harvest_job.source.config)
        sensor = self.source_config.get('sensor')

        ids = []

        if sensor == 'avhrr':
            glass_lai_prods = self.glassLAIavhrr()
        elif sensor == 'modis':
            glass_lai_prods = self.glassLAImodis()

        for result in glass_lai_prods:
            _id = self._create_object(sensor, result)
            if _id:
                ids.append(_id)

        return ids

    def fetch_stage(self, harvest_object):
        return True

    def import_stage(self, harvest_object):
        log = logging.getLogger(__name__ + '.import')
        log.debug('Import stage for package {}'
                  .format(harvest_object.id))

        self._set_source_config(harvest_object.job.source.config)
        self.obj = harvest_object

        if harvest_object.content is None:
            self._save_object_error('Empty content for object {}'
                                    .format(harvest_object.id),
                                    harvest_object, 'Import')
            return False

        # The OpenSearchHarvester will have assigned a status to each harvest
        # object.
        status = self._get_object_extra(harvest_object, 'status')

        # Check if we need to update the dataset
        if status != 'unchanged':
            # This can be a hook
            package = self._create_or_update_dataset(harvest_object, status)
            # This can be a hook
            if not package:
                return False
            package_id = package['id']
        else:
            package_id = harvest_object.package.id

        # Perform the necessary harvester housekeeping
        self._refresh_harvest_objects(harvest_object, package_id)

        # Finish up
        if status == 'unchanged':
            return 'unchanged'
        else:
            log.debug('Package {} was successully harvested.'
                      .format(package['id']))
            return True
