# -*- coding: utf-8 -*-

import logging
import json

from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class MELOAbaseHarvester(HarvesterBase):

    def _get_extra_fields(self, content, item):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN extra fields of the dataset.

        """

        # List of metadata fields to be ignored
        #ignore_list = []

        extras = content['extras']
        for field in extras:
            key = field['key']
        #    if key not in ignore_list:
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

        wavy_type = content['Wavy ID']

        collection_name = 'MELOA Wavy Measurements - ' + wavy_type
        item['collection_name'] = collection_name
        collection_id = collection_name.replace(' - ', '_').replace(' ', '_')
        item['collection_id'] = collection_id.upper()

        item['collection_description'] = '''In-situ Data collected using extra
         light surface drifters (WAVYs) in all water environments. The data
         produced have far-reaching applications, directly providing valuable
         information that will help to derive answers to diverse scientific,
         environmental and societal needs and achieving multiple objectives,
         from complementing observational gaps in ocean observation, to delivering
         validation datasets to satellite ground-truthing, along with the real
         possibility of their effective use by the common citizen. A dataset in
         MELOA is the collection of data samples acquired by a WAVY during a campaign.
         The contents of the data samples depend on the type of WAVY drifter. 
         The common information for all the types is the GNSS (Time, position,
         velocity and direction) and the battery power. This collection contains
         datasets clloected by a WAVY ''' + wavy_type + '.'

        return item

    def _get_tags_for_dataset(self, content):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN tags of the dataset.

        """

        tags_list = [{"name": "meloa"}, {"name": "wavy"}, {"name": content["Wavy ID"]}]

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
        item['timerange_start'] = item.pop('Measurements Start-time')
        item['timerange_end'] = item.pop('Measurements End-time')

        return item

    def _parse_resources(self, resources_content):
        resources = []

        for resource in resources_content:
            name = resource['name']
            url = resource['url']
            _format = resource['format']
            description = resource['description']
            if 'Measurements' in description:
                parsed_resource = self._make_measurements_resource(url,
                                                              name,
                                                              _format)
            elif 'Metadata' in description:
                parsed_resource = self._make_metadata_resource(url,
                                                               name,
                                                               _format)
            else:
                continue
            resources.append(parsed_resource)
        return resources

    def _make_metadata_resource(self, url, name, file_format):
        """
        Return a metadata resource dictionary.
        """
        description = 'Download the metadata files from MELOA catalogue.'  # noqa: E501
        mimetype = 'application/json'

        resource = {'name': name,
                    'description': description,
                    'url': url,
                    'format': file_format,
                    'mimetype': mimetype}

        return resource

    def _make_measurements_resource(self, url, name, file_format):
        """
        Return a measurements resource dictionary.
        """
        description = 'Download measurements file from MELOA catalogue.'
        mimetype = 'text/csv'

        resource = {'name': name,
                    'description': description,
                    'url': url,
                    'format': file_format,
                    'mimetype': mimetype}
        return resource

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        return parsed_content['resource']
