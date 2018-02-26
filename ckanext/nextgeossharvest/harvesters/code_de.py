# -*- coding: utf-8 -*-

import logging
import json
from datetime import datetime

from sqlalchemy import desc
from sqlalchemy.sql import update, bindparam

from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.model import HarvestJob
from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.opensearch_base import OpenSearchHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester
from ckanext.nextgeossharvest.lib.codede_base import CODEDEBase


class CODEDEHarvester(CODEDEBase, OpenSearchHarvester, NextGEOSSHarvester):
    """A Harvester for ESA Sentinel products available on CODE-DE."""
    implements(IHarvester)

    def info(self):
        return {
            'name': 'code-de',
            'title': 'CODE-DE Harvester',
            'description': 'A Harvester for Sentinel Products on CODE-DE'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'start_date' in config_obj:
                try:
                    datetime.strptime(config_obj['start_date'],
                                      '%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError:
                    raise ValueError('start_date format must be 2018-01-01T00:00:00.000Z')  # noqa: E501
            if 'end_date' in config_obj:
                try:
                    datetime.strptime(config_obj['end_date'],
                                      '%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError:
                    raise ValueError('end_date format must be 2018-01-01T00:00:00.000Z')  # noqa: E501
            for key in ('include_thumbnails', 'update_all'):
                if key in config_obj:
                    if not isinstance(config_obj[key], bool):
                        raise ValueError('{} must be boolean'.format(key))

        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.CODEDE.gather')
        log.debug('CODEDEHarvester gather_stage for job: %r', harvest_job)

        # Save a reference
        self.job = harvest_job

        self._set_source_config(self.job.source.config)

        # The CODE-DE results include rel="icon" links, but it seems like they
        # do not work. This setting lets us skip them now and grab them later.
        self.include_thumbnails = self.source_config('include_thumbnails',
                                                     False)

        self.update_all = self.source_config.get('update_all', False)

        # Used created rather than finished in case the most recent job
        # crashed
        last_job = Session.query(HarvestJob) \
            .filter(HarvestJob.source_id == self.job.source_id) \
            .order_by(desc(HarvestJob.created)).limit(1)[0]
        if last_job:
            last_date = datetime.strftime(last_job.created,
                                          '%Y-%m-%dT00:00:00.000Z')
        else:
            last_date = '*'
        log.debug('Last date is {}'.format(last_date))

        start_date = self.source_config.get('start_date', last_date)
        end_date = self.source_config.get('end_date', 'NOW')

        # Get the harvest_urls
        template = 'https://catalog.code-de.org/opensearch/request/?httpAccept=application/atom%2Bxml&parentIdentifier={}&startDate={}&endDate={}&maximumRecords=100'  # noqa: E501
        parent_identifiers = ['EOP:CODE-DE:S1_SAR_L1_GRD',
                              'EOP:CODE-DE:S1_SAR_L1_SLC',
                              'EOP:CODE-DE:S1_SAR_L2_OCN',
                              'EOP:CODE-DE:S2_MSI_L1C']
        harvest_urls = []
        for parent_identifier in parent_identifiers:
            harvest_urls.append(template.format(parent_identifier, start_date,
                                                end_date))

        self.os_id_name = 'dc:identifier',
        self.os_id_attr = {}
        self.os_id_mod = self.normalize_identifier
        self.os_guid_name = 'dc:identifier'
        self.os_guid_attr = {}
        self.os_guid_mod = self.normalize_identifier

        ids = []
        for harvest_url in harvest_urls:
            ids += self._crawl_results(harvest_url)

        return ids

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True

    def import_stage(self, harvest_object):
        log = logging.getLogger(__name__ + '.import')
        log.debug('Import stage for harvest object with GUID {}'
                  .format(harvest_object.id))

        # Save a reference
        self.obj = harvest_object

        if harvest_object.content is None:
            self._save_object_error('Empty content for object {}'
                                    .format(harvest_object.id),
                                    harvest_object, 'Import')
            return False

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
            package_id = self.obj.package.id

        # Flag the other objects of this source as not current anymore
        from ckanext.harvest.model import harvest_object_table
        u = update(harvest_object_table) \
            .where(harvest_object_table.c.package_id == bindparam('pkg_id')) \
            .values(current=False)
        Session.execute(u, params={'pkg_id': package_id})
        Session.commit()
        # Refresh current object from session, otherwise the
        # import paster command fails
        # (Copied from the Gemini harvester--not sure if necessary)
        self.obj.content = str(self.obj.content)
        Session.remove()
        Session.add(self.obj)
        Session.refresh(self.obj)

        # Set reference to package in the HarvestObject and flag it as
        # the current one
        if not self.obj.package_id:
            self.obj.package_id = package['id']

        self.obj.current = True
        self.obj.save()

        if status == 'unchanged':
            return 'unchanged'
        else:
            log.debug('Package {} was successully harvested.'
                      .format(package['id']))
            return True
