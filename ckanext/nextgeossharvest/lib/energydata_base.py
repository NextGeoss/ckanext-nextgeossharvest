# -*- coding: utf-8 -*-

import logging
import json
import os
import sys

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

        # Spatial Info. Entirety of Earth
        item['spatial'] = self._get_spatial_information(content)

        # Get timerange from different fields
        item['timerange_start'], item['timerange_end'] = self._get_timerange(content)
        

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

    def _get_timerange(self, content):
        if ('start_date', 'end_date') in content:
            if content['start_date'] and content['end_date']:
                return "{}-01-01T00:00:00Z".format(content['start_date']), "{}-12-31T23:59:59Z".format(content['end_date'])
        
            if content['start_date']:
                return "{}-01-01T00:00:00Z".format(content['start_date']), "{}-12-31T23:59:59Z".format(content['start_date'])

        if 'release_date' in content and content['release_date']:
            return "{}-01-01T00:00:00Z".format(content['release_date']), "{}-12-31T23:59:59Z".format(content['release_date'])
        
        return content['metadata_created'], content['metadata_created']

    def _get_spatial_information(self, content):
        """
        Get country borders from geojson file. ISO_A3 is used.
        """
        if 'country_code' in content and content['country_code']:
            # Full path to geojson file
            full_path = os.path.join(os.path.dirname(__file__), 'countries_iso_a3.geojson')

            with open(full_path) as countries_file:
                countries = json.load(countries_file)
                
                for country in countries:
                    if content['country_code'][0] == country:
                        return json.dumps(countries[country], ensure_ascii=False).encode('utf8')
            
            # Country code was not found inside the file
            return self._convert_to_geojson("POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90))")
        else:
            # API provided no country code
            return self._convert_to_geojson("POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90))")

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        return parsed_content['resource']
