# -*- coding: utf-8 -*-

import logging
import json

from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)

class EurogoosIntarosBaseHarvester(HarvesterBase):

    def _parse_content(self, entry):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """

        # content = json.loads(entry)
        log.debug("Products json: {}".format(entry))
        content = json.loads(entry)
        # content = content2.json()
        # content = entry.json()
        
        item = {}

        item['name'] = 'kappa'
        item['title'] = 'kappa2'
        item['notes'] = 'kappa3'
        item['tags'] = [{'tag1': 'sdf', 'tag2': 'gdfgd'}]
        item['resource'] = [content]


        item['Created'] = content['Created']
        item['Last Updated'] = content['Last Updated']
        if "Observing system name" in content:
            item['Observing system name'] = content['Observing system name']
        # if "Parameter name(s)" in content:
        #     item['Parameter name(s)'] = content['Parameter name(s)']
        item['Project/Program name(s)'] = content['Project/Program name(s)']
        item['Resources'] = content['Resources']
        if "Source" in content:
            item['Source'] = content['Source']
        if "Data Curator" in content:
            item['Data Curator'] = content['Data Curator']
        if "Principal Investigator" in content:
            item['Principal Investigator'] = content['Principal Investigator']
        if "Version" in content:
            item['Version'] = content['Version']

        return item

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        return parsed_content['resource']

    # def _parse_content(self, content):
