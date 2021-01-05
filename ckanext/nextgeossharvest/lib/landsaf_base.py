# -*- coding: utf-8 -*-

import logging
import json

from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class LandsafBaseHarvester(HarvesterBase):

    def _get_metadata_fields(self, content):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN package fields of the dataset.

        """

        item = {}
        
        # If a title is more than 100 characters (including the id),
        # 15 characters are being removed and the id is added again
        if len(content['title']) <= 100:
            item['title'] = content['title']
        else:
            dataset_id = content['title'].split('_')[-1]
            item['title'] = content['title'][85:] + "_" + dataset_id

        item['name'] = item['title']
        item['identifier'] = 'Landsaf-' + item['title'] 
        item['notes'] = content['description']

        item['timerange_start'] = content['date']
        item['timerange_end'] = content['date']

        item['collection_id'] = 'Landsaf_Products'
        item['collection_name'] = 'Landsaf Products'
        item['collection_description'] = """Eumetsat Land Cover Surface Analysis"""

        return item

    def _get_tags_for_dataset(self, tags):
        """
            Return a dictionary of metadata fields retrieved from
            the CKAN tags of the dataset.
        """

        tags_list = []

        for tag in tags:
            tags_list.append({"name": tag})

        return tags_list

    def _parse_content(self, json_content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """

        content = json.loads(json_content)

        item = self._get_metadata_fields(content)

        item['spatial'] = self._get_spatial_info(content['geometry'])

        item['tags'] = self._get_tags_for_dataset(content['tags'])

        item['resource'] = self._parse_resources(content['links'], content['images'])

        return item

    def _parse_resources(self, products, images):
        resources = []

        try:
            for img in images:
                if img['@type'] == 'thumbnail':
                    url = img['#text']
                    name = 'Thumbnail'

                    parsed_resource = self._make_thumbnail_resource(url, name)
                    resources.append(parsed_resource)
                    
                elif img['@type'] == 'overview':
                    url = img['#text']
                    name = 'Overview'
                    
                    parsed_resource = self._make_thumbnail_resource(url, name)
                    resources.append(parsed_resource)
        except TypeError:
            url = images['#text']
            name = 'Overview'

            parsed_resource = self._make_thumbnail_resource(url, name)
            resources.append(parsed_resource)
        
        for product in products:
            if '@title' in product:
                name = product['@title']
                url = product['@href']
                mimetype = product['@type']

                parsed_resource = self._make_product_resource(url, name, mimetype)

                resources.append(parsed_resource)
        
        return resources

    def _make_thumbnail_resource(self, url, name):
        """
        Return a thumbnail resource dictionary depending on the harvest source.
        """

        _format =  'image/gif'
        mimetype = 'image/gif'

        if url.endswith('.png'):
            _format =  'image/png'
            mimetype = 'image/png'

        resource = {'name': name,
                    'description': 'Download a product overview',
                    'url': url,
                    'format': _format,
                    'mimetype': mimetype}
        
        return resource

    def _make_product_resource(self, url, name, mimetype):
        """
        Return a product resource dictionary.
        """

        description = 'View product information.'

        if url.startswith("https://landsaf.ipma.pt/ChangeSystemProdLong"):
            description = 'Download product. Login is required'

        resource = {'name': name,
                    'description': description,
                    'url': url,
                    'format': mimetype,
                    'mimetype': mimetype}

        return resource

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        return parsed_content['resource']

    def _get_spatial_info(self, geometry):
        # If no geometry or wrong geometry is found,
        # the geometry of the whole Earth is inserted
        try:
            if (-180.0 <= float(geometry['westBL']) <= 180.0) :
                # Create WKT from Polygon Points
                spatial_wkt = "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))".format(
                    geometry['westBL'], geometry['northBL'], 
                    geometry['eastBL'], geometry['northBL'], 
                    geometry['eastBL'], geometry['southBL'], 
                    geometry['westBL'], geometry['southBL'], 
                    geometry['westBL'], geometry['northBL']
                )
            else:
                spatial_wkt = "POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90))"
        except (TypeError, ValueError):
            spatial_wkt = "POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90))"

        # WKT to geojson
        spatial_geojson = self._convert_to_geojson(spatial_wkt)

        return spatial_geojson
