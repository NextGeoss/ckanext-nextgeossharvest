# -*- coding: utf-8 -*-

from ckanext.harvest.harvesters.base import HarvesterBase
import logging
from string import Template

from bs4 import BeautifulSoup as Soup


log = logging.getLogger(__name__)


class OLUHarvester(HarvesterBase):

    def _normalize_names(self, item_node):
        """
        Return a dictionary of metadata fields with normalized names.

        The Sentinel entries are composed of metadata elements with names
        corresponding to the contents of name_elements and title, link,
        etc. elements. We can just extract all the metadata elements at
        once and rename them in one go.

        Note that elements like `ingestiondate`, which are included in the
        scihub results, will not be added to item as they are not part of the
        list of elements added in the original version.
        """
        spatial_dict = {'gmd:westboundlongitude': None,
                        'gmd:eastboundlongitude': None,
                        'gmd:southboundlatitude': None,
                        'gmd:northboundlatitude': None}

        # Since Micka Catalogue datasets refer to an entire day,
        # there is only one datastamp. Thus, this value will be
        # added to both timerange_start and timerange_end, and later
        # pos-processed
        normalized_names = {
            'gmd:datestamp': 'timerange_start',
            'gmd:westboundlongitude': 'spatial',
            'gmd:eastboundlongitude': 'spatial',
            'gmd:southboundlatitude': 'spatial',
            'gmd:northboundlatitude': 'spatial',
            'gmd:fileidentifier': 'identifier',
            'gmd:title': 'title',
            'gmd:abstract': 'notes',
            'gmd:parentidentifier': 'parentIdentifier'
        }
        item = {'spatial': spatial_dict}

        for subitem_node in item_node.findChildren():
            if subitem_node.name in normalized_names:
                key = normalized_names[subitem_node.name]
                if key:
                    if key == 'spatial':
                        item[key][subitem_node.name] = subitem_node.text
                    else:
                        item[key] = subitem_node.text

        # Since the spatial field is composed by 4 values,
        # if either of the values is None, then the whole field is dismissed
        for key in item['spatial']:
            if item['spatial'][key] is None:
                del item['spatial']
                break

        if ('timerange_start' in item) and ('timerange_end' not in item):
            item['timerange_end'] = item['timerange_start']

            if not item['timerange_start'].endswith('Z'):
                item['timerange_start'] += 'T00:00:00.000Z'
                item['timerange_end'] += 'T23:59:59.999Z'

        return item

    def _add_collection(self, item):
        """Return the item with collection ID, name, and description."""
        identifier = item['identifier'].lower()
        if identifier.startswith('olu'):
            item['collection_name'] = 'Open Land Use Map'  # noqa: E501
            item['collection_description'] = 'The main idea is to put Open Land Use dataset and also its metadata (from micka.lesprojekt.cz) in RDF format into Virtuoso and explore SPARQL queries that would combine data with metadata. For instance: Show me the datasets (municipalities) where more than 50% of the area is covered by residential areas and data were collected not later than 5 years ago? This query combines some metadata (such as year of data collection and municipality which data covers) with data itself  (object features with residential land use). For automatization of such queries it is necessary to have both data and metadata available for querying and interconnected. In ideal case the output will be endpoint where it will be possible to query both OLU data and metadata, some model queries and possibly some visualization of query results.'  # noqa: E501
            item['collection_id'] = 'OPEN_LAND_USE_MAP'

        return item

    def _get_tags_for_dataset(self, item):
        """Creates a list of tag dictionaries based on a product's metadata."""
        identifier = item['identifier'].lower()
        if identifier.startswith('olu'):
            tags = [{'name': 'OLU'}, {'name': 'open land use'},
                    {'name': 'LULC'}, {'name': 'land use'},
                    {'name': 'land cover'}, {'name': 'agriculture'}]

        else:
            tags = []
            log.debug('No tags for {}'.format(identifier))

        return tags

    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        soup = Soup(content, 'lxml')

        # Create an item dictionary and add metadata with normalized names.
        item = self._normalize_names(soup)

        # If there's a spatial element, convert it to GeoJSON
        # Remove it if it's invalid
        spatial_data = item.pop('spatial', None)
        if spatial_data:
            template = Template('''{"type": "Polygon", "coordinates": [$coords_list]}''')  # noqa: E501
            coords_NW = [float(spatial_data['gmd:westboundlongitude']), float(spatial_data['gmd:northboundlatitude'])]  # noqa: E501
            coords_NE = [float(spatial_data['gmd:eastboundlongitude']), float(spatial_data['gmd:northboundlatitude'])]  # noqa: E501
            coords_SE = [float(spatial_data['gmd:eastboundlongitude']), float(spatial_data['gmd:southboundlatitude'])]  # noqa: E501
            coords_SW = [float(spatial_data['gmd:westboundlongitude']), float(spatial_data['gmd:southboundlatitude'])]  # noqa: E501
            coords_list = [coords_NW, coords_NE, coords_SE, coords_SW, coords_NW]  # noqa: E501

            geojson = template.substitute(coords_list=coords_list)
            item['spatial'] = geojson

        name = item['identifier'].lower()
        item['name'] = name.replace('.', '_')

        resources = []
        # Thumbnail, alternative and enclosure
        quick_look = soup.find('gmd:md_browsegraphic')

        if quick_look:
            thumbnail = quick_look.find('gmd:filename').text
            if thumbnail:
                resources.append(self._make_thumbnail_resource(thumbnail))

        resources_list = soup.find_all({'gmd:online'})
        for resource in resources_list:
            name = resource.find('gmd:name').text.replace(' ', '_')
            if 'GeoJSON' in name:
                geojson_url = resource.find('gmd:linkage').text
                if geojson_url:
                    resources.append(self._make_geojson_resource(geojson_url))
            elif 'SHP' in name:
                shp_url = resource.find('gmd:linkage').text
                if shp_url:
                    resources.append(self._make_shp_resource(shp_url))
        resources.append(self._make_dataset_link(item['identifier']))
        # Add the collection info
        item = self._add_collection(item)

        item['tags'] = self._get_tags_for_dataset(item)
        item['resource'] = resources

        return item

    def _make_geojson_resource(self, url):
        """
        Return a geojson resource dictionary
        """
        name = 'GeoJSON Download from Plan4All'
        description = 'Download the geojson file from Plan4All.'  # noqa: E501
        _type = 'plan4all_geojson'

        geojson = {'name': name,
                   'description': description,
                   'url': url,
                   'format': 'JSON',
                   'mimetype': 'application/json',
                   'resource_type': _type}

        return geojson

    def _make_shp_resource(self, url):
        """
        Return a shapefile resource dictionary depending on the harvest source.
        """
        name = 'ShapeFile Download from Plan4All'
        description = 'Download ESRI shapefile as zip from Plan4All.'  # noqa: E501
        _type = 'plan4all_shapefile'

        shapefile = {'name': name,
                     'description': description,
                     'url': url,
                     'format': 'ZIP',
                     'mimetype': 'application/zip',
                     'resource_type': _type}

        return shapefile

    def _make_thumbnail_resource(self, url):
        """
        Return a thumbnail resource dictionary
        """
        name = 'Thumbnail Download'
        description = 'Download a PNG quicklook.'  # noqa: E501
        _type = 'thumbnail'

        thumbnail = {'name': name,
                     'description': description,
                     'url': url,
                     'format': 'PNG',
                     'mimetype': 'image/png',
                     'resource_type': _type}

        return thumbnail

    def _make_dataset_link(self, id):
        """
        Return a thumbnail resource dictionary
        """
        name = 'Original Metadata Record'
        description = 'Link to original metadata record.'
        url_base = 'https://micka.lesprojekt.cz/record/basic/'
        url = url_base + id
        _type = 'original_record'

        og_link = {'name': name,
                   'description': description,
                   'url': url,
                   'format': 'HTML',
                   'mimetype': 'text/html',
                   'resource_type': _type}

        return og_link

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        if self.obj.package and self.obj.package.resources:
            old_resources = [x.as_dict() for x in self.obj.package.resources]
        else:
            old_resources = None

        new_resources = parsed_content['resource']
        if not old_resources:
            resources = new_resources
        else:
            # Replace existing resources or add new ones
            new_resource_types = {x['resource_type'] for x in new_resources}
            resources = []
            for old in old_resources:
                old_type = old.get('resource_type')
                if old_type not in new_resource_types:
                    resources.append(old)
            resources += new_resources

        resources.sort(key=lambda x: x['name'])

        return resources
