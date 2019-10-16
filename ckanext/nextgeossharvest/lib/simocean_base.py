# -*- coding: utf-8 -*-

import logging
import json

from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class SIMOceanbaseHarvester(HarvesterBase):

    def _get_extra_fields(self, content, item):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN extra fields of the dataset.

        """

        # List of metadata fields to be ignored
        ignore_list = ['StartHour',
                       'StopHour',
                       'harvest_object_id',
                       'harvest_source_id',
                       'harvest_source_title']

        extras = content['extras']
        for field in extras:
            key = field['key']
            if key not in ignore_list:
                item[key] = field['value']

        return item

    def _get_metadata_fields(self, content):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN package fields of the dataset.

        """

        item = {}

        package_fields = ['notes',
                          'name',
                          'title']

        for field in package_fields:
            item[field] = content[field]

        item = self._get_extra_fields(content, item)

        return item

    def _get_collection(self, content, item):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN group field which can be mapped into collection.

        """

        # All datasets in SIMOcean catalogue belong into two groups,
        # the first is an encompassing collection (in-situ, model or satellite)
        # that "hosts" the other groups (collections)
        # In this harvester, only the 'in-situ' and 'model' groups are to
        # be harvested, since 'satellite' (CMEMS) is already being collected by
        # a different harvester

        group_list = ['in-situ', 'model']

        groups = content['groups']
        for group in groups:
            if group['name'] not in group_list:
                if group['display_name'] == 'Sea Surface Wind Forecats':
                    display_name = 'Sea Surface Wind Forecast'
                else:
                    display_name = group['display_name']

                collection_id = display_name.replace(' ', '_')
                collection_id = 'SIMOCEAN_' + collection_id.upper()

                item['collection_id'] = collection_id
                item['collection_name'] = display_name
                item['collection_description'] = group['description']

        return item

    def _get_tags_for_dataset(self, content):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN tags of the dataset.

        """

        tags_list = [{"name": "simocean"}]

        tags = content['tags']
        for tag in tags:
            tags_list.extend([{"name": tag['name']}])

        return tags_list

    def _parse_content(self, soup):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """

        content = json.loads(soup)
        item = self._get_metadata_fields(content)

        item = self._get_extra_fields(content, item)

        item = self._get_collection(content, item)

        item['tags'] = self._get_tags_for_dataset(content)

        item['resource'] = content['resources']

        # Add time range metadata that's not tied to product-specific fields
        # like StartTime so that we can filter by a dataset's time range
        # without having to cram other kinds of temporal data into StartTime
        # and StopTime fields, etc.
        item['timerange_start'] = item['StartTime']
        item['timerange_end'] = item['StopTime']

        return item

    def _make_resource(self, resource):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN tags of the dataset.

        """
        name = resource['name']

        if 'Product' in name:
            description = 'Download product from SIMOcean catalog'
            mimetype = resource['format']
            _format = 'NC'
        elif 'PNG' in name:
            description = 'Product preview'
            mimetype = 'image/png'
            _format = resource['format']
        elif 'XML' in name:
            description = 'Download the metadata manifest from SIMOcean catalog'  # noqa: E501
            mimetype = 'application/xml'
            _format = resource['format']
        else:
            return None

        ng_resource = {'name': name,
                       'description': description,
                       'url': resource['url'],
                       'format': _format,
                       'mimetype': mimetype}

        return ng_resource

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        if self.obj.package and self.obj.package.resources:
            old_resources = [x.as_dict() for x in self.obj.package.resources]
        else:
            old_resources = None

        resources = parsed_content['resource']

        new_resources = [self._make_resource(x) for x in resources if x]
        if not old_resources:
            resources = new_resources
        else:
            # Replace existing resources or add new ones
            new_resource_types = {x['resource_type'] for x in new_resources}
            resources = []
            for old in old_resources:
                old_type = old.get('resource_type')
                order = old.get('order')
                if old_type not in new_resource_types and order:
                    resources.append(old)
            resources += new_resources

            resources.sort(key=lambda x: x['order'])

        return resources
