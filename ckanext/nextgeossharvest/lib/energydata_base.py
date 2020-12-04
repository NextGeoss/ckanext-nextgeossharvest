# -*- coding: utf-8 -*-

import logging
import json

from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class EnergyDataBaseHarvester(HarvesterBase):

    def _get_metadata_fields(self, content):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN package fields of the dataset.
        """

        item = {}

        name = content['name']
        item['name'] = name
        item['title'] = name
        item['identifier'] = name

        item['notes'] = content['notes']

        item['Organization'] = content['organization']['title']

        return item

    def _get_collection(self, content, item):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN group field which can be mapped into collection.

        """

        collection_name = "EnergyData Collection"
        item['collection_name'] = collection_name
        collection_id = collection_name.replace('-', '_').replace(' ', '_')
        item['collection_id'] = collection_id.upper()
        item['collection_description'] = """ENERGYDATA.INFO is an open data platform providing access to datasets and data analytics 
that are relevant to the energy sector. ENERGYDATA.INFO has been developed as a public good to share data and analytics that 
can help achieving the United Nations’ Sustainable Development Goal 7 of ensuring access to affordable, reliable, sustainable 
and modern energy for all."""

        if item['notes'] is None or item['notes'] == "":
            item['notes'] = item['collection_description']

        return item

    def _get_tags_for_dataset(self, content, item):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN tags of the dataset.
        """

        tags_list = [{"name": "energydata"}]

        if 'tags' in content:
            for tag in content['tags']:
                if 'name' in tag:
                    tags_list.append({"name": tag['name']})

        return tags_list

    def _parse_content(self, soup):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """

        content = json.loads(soup)

        item = self._get_metadata_fields(content)

        item = self._get_collection(content, item)

        item['tags'] = self._get_tags_for_dataset(content, item)

        item['resource'] = self._parse_resources(content['resources'])

        item['timerange_start'] = content['metadata_created']
        item['timerange_end'] = content['metadata_modified']

        return item

    def _parse_resources(self, resources_content):
        resources = []

        for resource in resources_content:
            _format = resource['format']
            mimetype = resource['mimetype']

            if ".zip" in resource['url']:
                _format = "ZIP"
                mimetype = "ZIP"


            parsed_resource = {'name': resource['name'],
                        'description': resource['description'],
                        'url': resource['url'],
                        'format': _format,
                        'mimetype': mimetype}

            resources.append(parsed_resource)
        return resources

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        return parsed_content['resource']
