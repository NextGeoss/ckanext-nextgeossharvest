# -*- coding: utf-8 -*-

import logging
from string import Template

from bs4 import BeautifulSoup as Soup

from ckanext.harvest.harvesters.base import HarvesterBase


log = logging.getLogger(__name__)


class CMRHarvester(HarvesterBase):

    def _normalize_names(self, item_node):
        """
        Return a dictionary of metadata fields with normalized names.

        The CRM entries are composed of metadata elements with names
        corresponding to the contents of name_elements and title, link,
        etc. elements. We can just extract all the metadata elements at
        once and rename them in one go.
        """
        normalized_names = {
            'dc:date': 'timerange_start',
            'georss:polygon': 'spatial',
            'echo:producergranuleid': 'Filename',
            'echo:cloudcoverpercentage': 'CloudCoverage',
            'dc:identifier': 'identifier',
            'echo:granulesizemb': 'size',
            'echo:originalformat': 'File Original Format',
            'echo:datacenter': 'Data Center',
            'echo:coordinatesystem': 'CoordinateSystem'
        }

        item = {}
        for subitem_node in item_node.findChildren():
            if subitem_node.name in normalized_names:
                key = normalized_names[subitem_node.name]
                if key:
                    item[key] = subitem_node.text

        return item

    def _add_collection(self, item):
        """Return the item with collection ID, name, and description."""
        identifier = item['Filename'].lower()
        if identifier.startswith('myd13q1'):
            item['collection_id'] = 'MODIS_AQUA_MYD13Q1'
            item['collection_name'] = 'MODIS/Aqua Vegetation Indices 16-Day L3 Global 250m SIN Grid'  # noqa: E501
            item['collection_description'] = 'Global MYD13Q1 data are provided every 16 days at 250-meter spatial resolution as a gridded level-3 product in the Sinusoidal projection. Cloud-free global coverage is achieved by replacing clouds with the historical MODIS time series climatology record. Vegetation indices are used for global monitoring of vegetation conditions and are used in products displaying land cover and land cover changes.'  # noqa: E501

        elif identifier.startswith('myd13a1'):
            item['collection_id'] = 'MODIS_AQUA_MYD13A1'
            item['collection_name'] = 'MODIS/Aqua Vegetation Indices 16-Day L3 Global 500m SIN Grid'  # noqa: E501
            item['collection_description'] = 'Global MYD13A1 data are provided every 16 days at 500-meter spatial resolution as a gridded level-3 product in the Sinusoidal projection. Cloud-free global coverage is achieved by replacing clouds with the historical MODIS time series climatology record. Vegetation indices are used for global monitoring of vegetation conditions and are used in products displaying land cover and land cover changes.'  # noqa: E501

        elif identifier.startswith('myd13a2'):
            item['collection_id'] = 'MODIS_AQUA_MYD13A2'
            item['collection_name'] = 'MODIS/Aqua Vegetation Indices 16-Day L3 Global 1km SIN Grid'  # noqa: E501
            item['collection_description'] = 'Global MYD13A2 data are provided every 16 days at 1-kilometer spatial resolution as a gridded level-3 product in the Sinusoidal projection. Vegetation indices are used for global monitoring of vegetation conditions and are used in products displaying land cover and land cover changes.'  # noqa: E501

        elif identifier.startswith('mod13q1'):
            item['collection_id'] = 'MODIS_TERRA_MOD13Q1'
            item['collection_name'] = 'MODIS/Terra Vegetation Indices 16-Day L3 Global 250m SIN Grid'  # noqa: E501
            item['collection_description'] = 'Global MOD13Q1 data are provided every 16 days at 250-meter spatial resolution as a gridded level-3 product in the Sinusoidal projection. Cloud-free global coverage is achieved by replacing clouds with the historical MODIS time series climatology record. Vegetation indices are used for global monitoring of vegetation conditions and are used in products displaying land cover and land cover changes.'  # noqa: E501

        elif identifier.startswith('mod13a1'):
            item['collection_id'] = 'MODIS_TERRA_MOD13A1'
            item['collection_name'] = 'MODIS/Terra Vegetation Indices 16-Day L3 Global 500m SIN Grid'  # noqa: E501
            item['collection_description'] = 'Global MOD13A1 data are provided every 16 days at 500-meter spatial resolution as a gridded level-3 product in the Sinusoidal projection. Cloud-free global coverage is achieved by replacing clouds with the historical MODIS time series climatology record. Vegetation indices are used for global monitoring of vegetation conditions and are used in products displaying land cover and land cover changes.'  # noqa: E501

        elif identifier.startswith('mod13a2'):
            item['collection_id'] = 'MODIS_TERRA_MOD13A2'
            item['collection_name'] = 'MODIS/Terra Vegetation Indices 16-Day L3 Global 1km SIN Grid'  # noqa: E501
            item['collection_description'] = 'Global MOD13A2 data are provided every 16 days at 1-kilometer spatial resolution as a gridded level-3 product in the Sinusoidal projection. Vegetation indices are used for global monitoring of vegetation conditions and are used in products displaying land cover and land cover changes.'  # noqa: E501

        elif identifier.startswith('mod17a3h'):
            item['collection_id'] = 'MOD17A3H'
            item['collection_name'] = 'MODIS/Terra Net Primary Production Yearly L4 Global 500m SIN Grid'  # noqa: E501
            item['collection_description'] = 'The Terra/MODIS Gross Primary Productivity (GPP) product MOD17A3H is a cumulative composite of GPP values based on the radiation-use efficiency concept that is potentially used as inputs to data models to calculate terrestrial energy, carbon, water cycle processes, and biogeochemistry of vegetation. MOD17A3H is an annual composite at 500m spatial resolution delivered as a gridded Level-4 product in Sinusoidal projection.'  # noqa: E501

        elif identifier.startswith('mod17a2h'):
            item['collection_id'] = 'MOD17A2H'
            item['collection_name'] = 'MODIS/Terra Gross Primary Productivity 8-Day L4 Global 500m SIN Grid'  # noqa: E501
            item['collection_description'] = 'The Terra/MODIS Gross Primary Productivity (GPP) product MOD17A2H is a cumulative composite of GPP values based on the radiation-use efficiency concept that is potentially used as inputs to data models to calculate terrestrial energy, carbon, water cycle processes, and biogeochemistry of vegetation. MOD17A2H is an 8-day composite at 1-km spatial resolution delivered as a gridded Level-4 product in Sinusoidal projection.'  # noqa: E501

        elif identifier.startswith('myd15a2h'):
            item['collection_id'] = 'MODIS_AQUA_MYD15A2H'
            item['collection_name'] = 'MODIS/Aqua Leaf Area Index/FPAR 8-Day L4 Global 500m SIN Grid'  # noqa: E501
            item['collection_description'] = 'The Level-4 MODIS global Leaf Area Index (LAI) and Fraction of Photosynthetically Active Radiation (FPAR) product is a 8-day 500-meter resolution product on a Sinusoidal grid. Science Data Sets provided in the MYD15A2H include LAI, FPAR, a quality rating, and standard deviation for each variable.'  # noqa: E501

        elif identifier.startswith('mod15a2h'):
            item['collection_id'] = 'MODIS_TERRA_MOD15A2H'
            item['collection_name'] = 'MODIS/Terra Leaf Area Index/FPAR 8-Day L4 Global 500m SIN Grid'  # noqa: E501
            item['collection_description'] = 'The Level-4 MODIS global Leaf Area Index (LAI) and Fraction of Photosynthetically Active Radiation (FPAR) product is a 8-day 500-meter resolution product on a Sinusoidal grid. Science Data Sets provided in the MOD15A2H include LAI, FPAR, a quality rating, and standard deviation for each variable.'  # noqa: E501

        elif identifier.startswith('mod14a2'):
            item['collection_id'] = 'MODIS_TERRA_MOD14A2'
            item['collection_name'] = 'MODIS/Terra Thermal Anomalies/Fire 8-Day L3 Global 1km SIN Grid'  # noqa: E501
            item['collection_description'] = 'MOD14A2 data are 8-day fire-mask composites at 1-kilometer resolution provided as a gridded level-3 product in the Sinusoidal projection. Science Data Sets include the fire-mask and algorithm quality assurance.'  # noqa: E501

        elif identifier.startswith('myd14a2'):
            item['collection_id'] = 'MODIS_AQUA_MYD14A2'
            item['collection_name'] = 'MODIS/Aqua Thermal Anomalies/Fire 8-Day L3 Global 1km SIN Grid'  # noqa: E501
            item['collection_description'] = 'MYD14A2 data are 8-day fire-mask composites at 1-kilometer resolution provided as a gridded level-3 product in the Sinusoidal projection. Science Data Sets include the fire-mask and algorithm quality assurance.'  # noqa: E501

        return item

    def _get_tags_for_dataset(self, item):
        """Creates a list of tag dictionaries based on a product's metadata."""
        identifier = item['Filename'].lower()
        if identifier.startswith('myd13q1') or identifier.startswith('myd13a1') or identifier.startswith('myd13a2') or identifier.startswith('mod13q1') or identifier.startswith('mod13a1') or identifier.startswith('mod13a2'):  # noqa: E501
            tags = [{'name': 'climate change'}, {'name': 'canopy characteristics'}, {'name': 'biomass'}, {'name': 'vegetation index'}, {'name': 'plant phenology'}, {'name': 'growing season'}, {'name': 'EVI'}, {'name': 'NVDI'}]  # noqa: E501

        elif identifier.startswith('mod17a3h') or identifier.startswith('mod17a2h'):  # noqa: E501
            tags = [{'name': 'climate change'}, {'name': 'climate modeling'}, {'name': 'biomass'}, {'name': 'NPP'}, {'name': 'GPP'}]  # noqa: E501

        elif identifier.startswith('myd15a2h') or identifier.startswith('mod15a2h'):  # noqa: E501
            tags = [{'name': 'climate change'}, {'name': 'canopy characteristics'}, {'name': 'biomass'}, {'name': 'evapotranspiration'}, {'name': 'plant phenology'}, {'name': 'growing season'}, {'name': 'forest composition'}, {'name': 'vegetation structure'}, {'name': 'leaf characteristics'}, {'name': 'photosynthetically active radiation'}, {'name': 'FPAR'}, {'name': 'LAI'}]  # noqa: E501

        elif identifier.startswith('myd14a2') or identifier.startswith('mod14a2'):  # noqa: E501
            tags = [{'name': 'climate change'}, {'name': 'land surface temperature'}, {'name': 'fires'}]  # noqa: E501

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

        start_stop_time = item.pop('timerange_start', None)
        if start_stop_time:
            item['timerange_start'] = start_stop_time.split('/')[0]
            item['timerange_end'] = start_stop_time.split('/')[1]

        coordinates = item.pop('spatial', None)
        if coordinates:
            template = Template('''{"type": "Polygon", "coordinates": [$coords_list]}''')  # noqa: E501
            coord_str_list = coordinates.split(' ')
            coords_list = []
            for idx_list in range(len(coord_str_list) / 2):
                lat = coord_str_list[idx_list * 2 + 0]
                lng = coord_str_list[idx_list * 2 + 1]
                point = [float(lng), float(lat)]
                coords_list.append(point)

            geojson = template.substitute(coords_list=coords_list)
            item['spatial'] = geojson

        item['name'] = item['identifier'].lower()

        # Convert size (298.74 MB to an integer representing bytes)
        size = int(float(item['size']) * 1000000)

        resources = []
        # Thumbnail, manifest and enclosure
        enclosure_field = soup.find('link', rel='enclosure')
        if enclosure_field:
            url = enclosure_field['href']
            resources.append(self._make_product_resource(url, size))

        manifest = soup.find('link', rel='via', title='Product metadata')['href']  # noqa: E501
        resources.append(self._make_manifest_resource(manifest))

        icon_list = soup.find_all('link', rel='icon')
        thumbnail_list = []
        for icon in icon_list:
            thumbnail_list.append(icon['href'])

        identifier = item['Filename'].lower()
        if identifier.startswith('myd13q1') or identifier.startswith('myd13a1') or identifier.startswith('myd13a2') or identifier.startswith('mod13q1') or identifier.startswith('mod13a1') or identifier.startswith('mod13a2'):  # noqa: E501
            resources.append(self._make_thumbnail_resource(thumbnail_list[0],
                                                           'evi'))
            resources.append(self._make_thumbnail_resource(thumbnail_list[1],
                                                           'ndvi'))
        elif identifier.startswith('mod17a2h'):  # noqa: E501
            resources.append(self._make_thumbnail_resource(thumbnail_list[0],
                                                           'thumbnail_1'))
            resources.append(self._make_thumbnail_resource(thumbnail_list[1],
                                                           'thumbnail_2'))
        elif identifier.startswith('myd15a2h') or identifier.startswith('mod15a2h'):  # noqa: E501
            resources.append(self._make_thumbnail_resource(thumbnail_list[0],
                                                           'lai'))
            resources.append(self._make_thumbnail_resource(thumbnail_list[1],
                                                           'fpar'))
        elif identifier.startswith('myd14a2') or identifier.startswith('mod14a2') or identifier.startswith('mod17a3h'):  # noqa: E501
            resources.append(self._make_thumbnail_resource(thumbnail_list[0],
                                                           'thumbnail_1'))

        item['resource'] = resources

        # Add the collection info
        item = self._add_collection(item)

        # The title of the dataset is set the same as the
        # filename without the extension
        title = (item['Filename'].split('.hdf')[0]).replace('.', '_')  # noqa: E501
        item['title'] = title

        item['notes'] = item['collection_description']

        item['tags'] = self._get_tags_for_dataset(item)

        # Remove fields from package dictionary
        item.pop('Filename')
        item.pop('size')
        return item

    def _make_manifest_resource(self, url):
        """
        Return a manifest resource dictionary depending on the harvest source.
        """

        name = 'Product Metadata Download'
        description = 'Download the metadata manifest from CMR Common Metadata Repository'  # noqa: E501
        _type = 'manifest'

        manifest = {'name': name,
                    'description': description,
                    'url': url,
                    'format': 'XML',
                    'mimetype': 'application/xml',
                    'resource_type': _type}

        return manifest

    def _make_thumbnail_resource(self, url, thumbnail_type):
        """
        Return a thumbnail resource dictionary depending on the harvest source.
        """
        if thumbnail_type == 'evi':
            name = 'EVI Thumbnail Download'
            description = 'Download the EVI layer thumbnail from NASA\'s LP DAAC'  # noqa: E501
            _type = 'evi_thumbnail'
        elif thumbnail_type == 'ndvi':
            name = 'NVDI Thumbnail Download'
            description = 'Download the NVDI layer thumbnail from NASA\'s Land Processes Distributed Active Archive Center (LP DAAC)'  # noqa: E501
            _type = 'nvdi_thumbnail'
        elif thumbnail_type == 'lai':
            name = 'LAI Thumbnail Download'  # noqa: E501
            description = 'Download the LAI layer thumbnail from NASA\'s Land Processes Distributed Active Archive Center (LP DAAC)'  # noqa: E501
            _type = 'lai_thumbnail'
        elif thumbnail_type == 'fpar':
            name = 'FPAR Thumbnail Download'  # noqa: E501
            description = 'Download the FPAR layer thumbnail from NASA\'s Land Processes Distributed Active Archive Center (LP DAAC)'  # noqa: E501
            _type = 'fpar_thumbnail'
        elif thumbnail_type == 'thumbnail_1':
            name = 'Thumbnail-1 Download'
            description = 'Download thumbnail from NASA\'s Land Processes Distributed Active Archive Center (LP DAAC)'  # noqa: E501
            _type = 'thumbnail_1'
        elif thumbnail_type == 'thumbnail_2':
            name = 'Thumbnail-2 Download'
            description = 'Download thumbnail from NASA\'s Land Processes Distributed Active Archive Center (LP DAAC)'  # noqa: E501
            _type = 'thumbnail_2'
        else:
            return None

        thumbnail = {'name': name,
                     'description': description,
                     'url': url,
                     'format': 'JPEG',
                     'mimetype': 'image/jpeg',
                     'resource_type': _type}

        return thumbnail

    def _make_product_resource(self, url, size):
        """
        Return a product resource dictionary depending on the harvest source.
        """

        name = 'Product Download'
        description = 'Download product from NASA\'s Land Processes Distributed Active Archive Center (LP DAAC). NOTE: DOWNLOAD REQUIRES LOGIN'  # noqa: E501
        _type = 'product'

        product = {'name': name,
                   'description': description,
                   'url': url,
                   'format': 'HDF',
                   'mimetype': 'application/x-hdfeos',
                   'size': size,
                   'resource_type': _type}

        return product

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
