# -*- coding: utf-8 -*-

import logging
import json
from datetime import datetime

from sqlalchemy import desc

from ckan.model import Session
from ckan.plugins.core import implements

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra
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
            if 'datasets_per_job' in config_obj:
                limit = config_obj['datasets_per_job']
                if not isinstance(limit, int) and not limit > 0:
                    raise ValueError('datasets_per_job must be a positive integer')  # noqa: E501
            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                if not isinstance(timeout, int) and not timeout > 0:
                    raise ValueError('timeout must be a positive integer')
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
        self.include_thumbnails = self.source_config.get('include_thumbnails',
                                                         False)

        self.update_all = self.source_config.get('update_all', False)

        # If we need to restart, we can do so from the ingestion timestamp
        # of the last harvest object for the source. So, query the harvest
        # object table to get the most recently created harvest object
        # and then get its restart_date extra, and use that to restart
        # the queries
        parent_identifiers = ['EOP:CODE-DE:S1_SAR_L1_GRD',
                              'EOP:CODE-DE:S1_SAR_L1_SLC',
                              'EOP:CODE-DE:S1_SAR_L2_OCN',
                              'EOP:CODE-DE:S2_MSI_L1C']
        restart_dates = []
        for parent_identifier in parent_identifiers:
            last_object = Session.query(HarvestObject).\
                join(HarvestObjectExtra).\
                filter(HarvestObject.harvest_source_id == self.job.source_id).\
                filter(HarvestObjectExtra.value == parent_identifier).\
                order_by(desc(HarvestObject.gathered)).limit(1)
            if last_object:
                last_object = last_object[0]
                restart_date = self._get_object_extra(last_object,
                                                      'restart_date', '')
                restart_dates.append(restart_date)
            else:
                restart_dates.append('')

        # Make the harvest URLs
        template = 'https://catalog.code-de.org/opensearch/request/?httpAccept=application/atom%2Bxml&parentIdentifier={}&startDate={}&endDate={}&maximumRecords=100'  # noqa: E501
        harvest_urls = []

        for index, parent_identifier in enumerate(parent_identifiers):
            start_date = self.source_config.get('start_date',
                                                restart_dates[index])
            end_date = self.source_config.get('end_date', '')
            harvest_urls.append(template.format(parent_identifier, start_date,
                                                end_date))

        self.os_id_name = 'dc:identifier'
        self.os_id_attr = {}
        self.os_id_mod = self.normalize_identifier
        self.os_guid_name = 'dc:identifier'
        self.os_guid_attr = {}
        self.os_guid_mod = self.normalize_identifier
        self.os_restart_date_name = 'dc:date'
        self.os_restart_date_attr = {}
        self.os_restart_date_mod = self._get_end_date
        self.flagged_extra = 'codede_download_url'

        ids = []

        # Set the limit for the maximum number of results per job.
        # Since the new harvester jobs will be created on a rolling basis
        # via cron jobs, we don't need to grab all the results from a date
        # range at once and the harvester will pickup from the last gathered
        # date each time it runs.
        limit = self.source_config.get('datasets_per_job', 1000)
        timeout = self.source_config.get('timeout', 4)

        for index, harvest_url in enumerate(harvest_urls):
            self.os_restart_filter = parent_identifiers[index]
            ids += self._crawl_results(harvest_url, limit, timeout)

        return ids

    def fetch_stage(self, harvest_object):
        """Fetch was completed during gather."""
        return True

    def import_stage(self, harvest_object):
        log = logging.getLogger(__name__ + '.import')
        log.debug('Import stage for harvest object with GUID {}'
                  .format(harvest_object.id))

        # Save a reference (review the utility of this)
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
            package_id = harvest_object.package.id

        # Perform the necessary harvester housekeeping
        self._refresh_harvest_objects(harvest_object, package_id)

        if status == 'unchanged':
            return 'unchanged'
        else:
            log.debug('Package {} was successully harvested.'
                      .format(package['id']))
            return True
