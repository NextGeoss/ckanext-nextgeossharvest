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
                       'harvest_source_title',
                       'Limitations On Public Access',
                       'CreationTime',
                       'Responsible Party',
                       'Metadata Date']

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

        item['resource'] = self._parse_resources(content['resources'])

        # Rename StartTime and StopTime to timerange_start
        # and timerange_end, respectively and remove former
        # from the package
        item['timerange_start'] = item.pop('StartTime')
        item['timerange_end'] = item.pop('StopTime')

        return item

    def _parse_resources(self, resources_content):
        resources = []

        for resource in resources_content:
            name = resource['name']
            url = resource['url']
            _format = resource['format']
            if 'Product' in name:
                parsed_resource = self._make_product_resource(url,
                                                              name,
                                                              _format)
            elif 'PNG' in name:
                parsed_resource = self._make_thumbnail_resource(url,
                                                                name,
                                                                _format)
            elif 'XML' in name:
                parsed_resource = self._make_manifest_resource(url,
                                                               name,
                                                               _format)
            else:
                continue
            resources.append(parsed_resource)
        return resources

    def _make_manifest_resource(self, url, name, file_format):
        """
        Return a manifest resource dictionary.
        """
        description = 'Download the metadata manifest from SIMOcean catalog'  # noqa: E501
        mimetype = 'application/xml'

        resource = {'name': name,
                    'description': description,
                    'url': url,
                    'format': file_format,
                    'mimetype': mimetype}

        return resource

    def _make_thumbnail_resource(self, url, name, file_format):
        """
        Return a thumbnail resource dictionary depending on the harvest source.
        """
        description = 'Product preview'
        mimetype = 'image/png'

        resource = {'name': name,
                    'description': description,
                    'url': url,
                    'format': file_format,
                    'mimetype': mimetype}
        return resource

    def _make_product_resource(self, url, name, mimetype):
        """
        Return a product resource dictionary.
        """
        description = 'Download product from SIMOcean catalog'
        file_format = 'NC'

        resource = {'name': name,
                    'description': description,
                    'url': url,
                    'format': file_format,
                    'mimetype': mimetype}
        return resource

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        return parsed_content['resource']
