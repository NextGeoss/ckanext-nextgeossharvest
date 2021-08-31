# -*- coding: utf-8 -*-

import logging
import json

from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class FaoBaseHarvester(HarvesterBase):

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

        if item['title'].startswith('fao'):
            item['identifier'] = item['title']
        else:
            item['identifier'] = 'fao_' + item['title']
        
        item['notes'] = content['description']

        item['timerange_start'] = content['date']
        item['timerange_end'] = content['date']

        item['collection_id'] = 'FAO_CORE_DATASETS'
        item['collection_name'] = 'FAO Core Datasets'
        item['collection_description'] = """FAO produces a large number of Geographic Information System (GIS) datasets for 
monitoring, assessment and analysis of environmental and socio-economic factors causing poverty and food insecurity. 
Particular relevance is given to malnutrition, farming systems and crops, livestock production systems, fishery and forestry sectors, agro-ecological zoning, 
land and water resources management and climate related issues."""

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

                # Fix broken WMS urls
                if '/geoserver/wms/' in url and not 'request=GetCapabilities' in url and not 'service=WMS' in url:
                    url += '&request=GetCapabilities&service=WMS'

                if '/map/gsrv/gsrv1/' in url and '/wms' in url and not 'request=GetCapabilities' in url and not 'service=WMS' in url:
                    url += '?request=GetCapabilities&service=WMS'

                # Skip images with www:download-1.0-http--download mimetype
                # due to them not working most of the time
                if mimetype == 'WWW:DOWNLOAD-1.0-http--download':
                    continue

                parsed_resource = self._make_product_resource(url, name, mimetype)

                resources.append(parsed_resource)
        
        return resources

    def _make_thumbnail_resource(self, url, name):
        """
        Return a thumbnail resource dictionary depending on the harvest source.
        """

        resource = {'name': name,
                    'description': 'Product preview in PNG format',
                    'url': url,
                    'format': 'image/png',
                    'mimetype': 'image/png'}
        
        return resource

    def _make_product_resource(self, url, name, mimetype):
        """
        Return a product resource dictionary.
        """

        description = 'View product'

        # Mimetype is defined wrongly most of the time
        # In these cases, the url is used
        if mimetype.endswith('xml') or 'XML' in name or 'request=GetFeature' in url:
            mimetype = 'XML'
            description = 'View product on FAO OGC Service'
        if ".zip" in url or "=SHAPE-ZIP" in url:
            mimetype = 'ZIP'
            description = 'Download product from FAO catalog'
        if ".jpg" in url:
            mimetype = 'JPG'
            description = 'Download product from FAO catalog'
        if ".kml" in url:
            mimetype = 'KML'
            description = 'Download product from FAO catalog'
        if "wms" in url:
            mimetype = 'WMS'
            description = 'View product on FAO OGC Service'
        if "/ows/" in url:
            mimetype = 'HTML'
            description = 'View product'


        if url.endswith('?SERVICE=WMS'):
            url += '&request=GetCapabilities'

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
