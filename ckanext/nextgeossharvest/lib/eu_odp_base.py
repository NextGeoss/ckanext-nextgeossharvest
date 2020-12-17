# -*- coding: utf-8 -*-

import logging
import json
import stringcase
import re

from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)

class EuOdpBaseHarvester(HarvesterBase):

    def _get_metadata_fields(self, content):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN package fields of the dataset.
        """

        item = {}
        log.debug('check content')
        # log.debug(content)
        # dirty_name = content['result']['results']['dataset']['uri']
        dirty_name = content['dataset']['uri']

        name = self.get_clean_snakecase(dirty_name)

        item['name'] = name
        item['title'] = name
        item['identifier'] = name

        # item['notes'] = content['notes']
        item['notes'] = name

        # item['Organization'] = content['organization']['title']
        item['Organization'] = name


        return item

    def _get_collection(self, content, item):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN group field which can be mapped into collection.
        """

        collection_name = "European Union Open Data Portal Collection"
        item['collection_name'] = collection_name
        # collection_id = collection_name.replace('-', '_').replace(' ', '_')
        # item['collection_id'] = collection_id.upper()
        item['collection_id'] = 'EUODP'
        item['collection_description'] = "Data collected through the https://data.europa.eu/euodp/data/ website."

        if item['notes'] is None or item['notes'] == "":
            item['notes'] = item['collection_description']

        return item

    def _get_tags_for_dataset(self, content, item):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN tags of the dataset.
        """

        tags_list = [{"name": "euodp"}]

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

        item['resource'] = self._parse_resources(content['dataset'])

        # item['timerange_start'] = content['metadata_created']
        # item['timerange_end'] = content['metadata_modified']

        return item

    def get_clean_snakecase(self, og_string):
        og_string = re.sub('[^0-9a-zA-Z]+', '_', og_string).lower()
        sc_string = stringcase.snakecase(og_string).strip("_")
        while "__" in sc_string:
            sc_string = sc_string.replace("__", "_")
        return sc_string


    def _parse_resources(self, resources_content):
        resources = []

        if len(resources_content['title_dcterms']) > 0:
            for i in range(len(resources_content['distribution_dcat'])):
                try:
                    log.debug(resources_content)
                    dirty_name = resources_content['title_dcterms'][i]['value_or_uri']
                    log.debug(dirty_name)
                    name = self.get_clean_snakecase(dirty_name)
                    log.debug('dirtyname')
                    log.debug(name)

                    parsed_resource = {
                                    'name': name,
                                    'description': resources_content['distribution_dcat'][i]['description_dcterms'][0]['value_or_uri'],
                                    'url': resources_content['distribution_dcat'][i]['accessURL_dcat'][0]['uri'],
                                    # 'url': name,
                                    'format': resources_content['distribution_dcat'][i]['format_dcterms']['uri'].split('/')[-1]}

                    resources.append(parsed_resource)
                except KeyError, IndexError:
                    continue
        else:
            log.debug('else check')
            log.debug(resources_content)
        return resources

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        return parsed_content['resource']