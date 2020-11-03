# -*- coding: utf-8 -*-

import logging
import json

from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class NoaGeobservatoryBaseHarvester(HarvesterBase):
    
    def _parse_content(self, entry):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        item = {}
        
        content = json.loads(entry)
        
        item['name'] = content['imgtif'].split('/')[1].lower() + "_" + content['type']+ "_" + str(content['intid'])
        item['title'] = item['name']
        item['notes'] = "For more information visit: geobservatory.beyond-eocenter.eu  Part of Earthquake Event with Code: {}  The GeObservatory is activated in major geohazard events (earthquakes, volcanic activity, landslides,etc.) and automatically produces a series of Sentinel-1 based co-event interferograms (DInSAR) to map the surface deformation associated with the event. It also produces pre-event interferograms to be used as a benchmark.".format(content['eventid'])

        item['spatial'] = content['spatial']
        
        item['tags'] = [{"name": "noa_geobservatory"}]

        item['identifier'] = item['name']
        
        # Resources
        item['resource'] = [
                    #TIF File
                    {'name': content['imgtif'].split('/')[-1],
                    'description': "Download the product from NOA GeObservatory.",
                    'url': "http://geobservatory.beyond-eocenter.eu/{}".format(content['imgtif']),
                    'format': "tif",
                    'mimetype': "tif"},
                    #Low Res PNG
                    {'name': content['imglow'].split('/')[-1],
                    'description': "Low Resolution Preview.",
                    'url': "http://geobservatory.beyond-eocenter.eu/{}".format(content['imglow']),
                    'format': "PNG",
                    'mimetype': "PNG"},
                    {'name': content['kml'].split('/')[-1],
                    'description': "KML File.",
                    'url': "http://geobservatory.beyond-eocenter.eu/{}".format(content['kml']),
                    'format': "KML",
                    'mimetype': "KML"},
                    ]

        item['timerange_start'] = str(content['slave'])
        item['timerange_end'] = str(content['slave'])

        item['collection_id'] = 'NOA_INTERFEROGRAMS'
        item['collection_name'] = 'NOA Interferograms'  # noqa: E501
        item['collection_description'] = 'Sentinel-1 based pre-event and co-event interferograms'  # noqa: E501

        return item


    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        return parsed_content['resource']
