# -*- coding: utf-8 -*-

import logging
import json

from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class JrcEmisBaseHarvester(HarvesterBase):
    
    def _parse_content(self, entry):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """

        content = json.loads(entry)
        
        item = {}

        item = self._parse_content(content)

        return item

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        return parsed_content['resource']

    def _parse_content(self, content):
        item['name'] = content['title']
        item['title'] = content['title']
        item['spatial'] = content['Geographic bounding box']
        item['lineage'] = content['Lineage']
        item['data theme'] = content['Data theme(s)']
        item['issue date'] = content['Issue date']
        item['language'] = content['Language']
        item['last modified'] = content['Last modified']
        item['coordinate system'] = content['Coordinate Reference System']
        item['eurovoc'] = content['Eurovoc concept(s)']
        item['identified'] = content['Identifier']
        item['landing page'] = content['Landing page']
        item['coverage'] = content['Temporal coverage']
        
