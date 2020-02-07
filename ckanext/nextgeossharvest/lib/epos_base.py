# -*- coding: utf-8 -*-

import logging

from bs4 import BeautifulSoup as Soup
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class EPOSbaseHarvester(HarvesterBase):

    def _normalize_names(self, item_node):
        """
        Return a dictionary of metadata fields with normalized names.
        """

        normalized_names = {
            'date': 'timerange_start',
            'spatial': 'spatial',
            'filename': 'Filename',
            'eop:platform': 'FamilyName',
            'eop:instrument': {'eop:shortname': 'InstrumentShortName', 'eop:description': 'InstrumentName'},  # noqa: E501
            'sar:polarisationchannels': 'PolarisationChannels',
            'eop:operationalmode': 'InstrumentMode',
            'eop:sensortype': 'InstrumentFamilyName',
            'sar:antennalookdirection': 'AntennaLookDirection',
            'eop:status': 'Status',
            'eop:statussubtype': 'StatusSubType',
            'eop:imagequalitystatus': 'ImageQualityStatus',
            'eop:processingcenter': 'ProcessingCenter',
            'eop:processingdate': 'ProcessingDate',
            'eop:method': 'ProcessingMethod',
            'producttype': 'ProductType',
            'eop:orbitnumber': 'RelativeOrbitNumber',
            'eop:lastorbitnumber': 'LastOrbitNumber',
            'eop:productqualitystatus': 'ProductQualityStatus',
            'eop:acquisitiontype': 'AcquisitionType',
            'eop:orbitdirection': 'OrbitDirection',
            'eop:processorname': 'ProcessorName',
            'eop:processorversion': 'ProcessorVersion',
            'eop:processorlevel': 'ProcessorLevel',
            'identifier': 'identifier',
            'title': 'title',
            'eop:size': 'size',
            'eop:nativeproductformat': 'NativeProductFormat'
        }
        item = {}

        for subitem_node in item_node.findChildren():
            if subitem_node.name in normalized_names:
                # eop:instrument as to be treated differently due to the fact
                # that the shortname tag has multi-appearances
                if subitem_node.name == 'eop:instrument':
                    for field in subitem_node:
                        if field.name in normalized_names[subitem_node.name]:
                            key = normalized_names[subitem_node.name][field.name]   # noqa: E501
                            if key:
                                item[key] = field.text
                else:
                    key = normalized_names[subitem_node.name]
                    if key and subitem_node.text != '':
                        item[key] = subitem_node.text

        return item

    def _add_collection(self, item):
        """Return the item with collection ID, name, and description."""
        prodType = item['ProductType']
        if prodType.startswith('UNWRAPPED_INTERFEROGRAM'):
                item['collection_id'] = prodType
                item['collection_name'] = 'Unwrapped Interferogram'  # noqa: E501
                item['collection_description'] = 'Unwrapped Differential SAR Interferogram (Phase and Amplitude).'  # noqa: E501
        elif prodType.startswith('WRAPPED_INTERFEROGRAM'):
                item['collection_id'] = prodType
                item['collection_name'] = 'Wrapped Interferogram'  # noqa: E501
                item['collection_description'] = 'Differential SAR Interferogram (Phase and Amplitude) in the wrapped domain.'  # noqa: E501
        elif prodType.startswith('SPATIAL_COHERENCE'):
                item['collection_id'] = prodType
                item['collection_name'] = 'Spatial Coherence'  # noqa: E501
                item['collection_description'] = 'Interferometric SAR Spatial coherence.'  # noqa: E501
        elif prodType.startswith('LOS_DISPLACEMENT_TIMESERIES'):
                item['collection_id'] = prodType
                item['collection_name'] = 'LoS Displacement Timeseries'  # noqa: E501
                item['collection_description'] = 'Displacement Time Series along the sensor Line of Sight.'  # noqa: E501
        elif prodType.startswith('INTERFEROGRAM_APS_GLOBAL_MODEL'):
                item['collection_id'] = prodType
                item['collection_name'] = 'Interferogram APS Global Model'  # noqa: E501
                item['collection_description'] = 'Interferometric Atmospheric Phase Screen derived from Global Atmospheric Model.'  # noqa: E501
        elif prodType.startswith('MAP_OF_LOS_VECTOR'):
                item['collection_id'] = prodType
                item['collection_name'] = 'Map of LoS Vector'  # noqa: E501
                item['collection_description'] = 'Map of the Line of Sight vector (North East Up coefficients).'  # noqa: E501
        else:  # add missing collections
            message = 'No collection for product {}'.format(prodType)
            log.warning(message)

        return item

    def _get_tags_for_dataset(self, item):
        """Creates a list of tag dictionaries based on a product's metadata."""
        tags = [{'name': 'EPOS-Sat'}]

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
        geojson = self._convert_to_geojson(item.pop('spatial', None))
        if geojson:
            item['spatial'] = geojson

        date_tmp = item.pop('timerange_start', None)
        if date_tmp:
            date_tmp = date_tmp.split('/')

            start_tmp = date_tmp[0].split('.')
            ms = start_tmp[1][0:3] + 'Z'
            item['timerange_start'] = start_tmp[0] + '.' + ms

            stop_tmp = date_tmp[1].split('.')
            ms = stop_tmp[1][0:3] + 'Z'
            item['timerange_end'] = stop_tmp[0] + '.' + ms

        item['name'] = item['identifier'].lower()
        item['name'] = item['name'].replace('-', '_')

        if 'size' in item:
            size = item.pop('size')
        else:
            size = None

        # Thumbnail and enclosure
        enclosure = soup.find('link', rel='enclosure')
        thumbnail = soup.find('link', rel='icon')

        resources = []
        if enclosure:
            product_url = enclosure['href']
            product_resource = self._make_product_resource(product_url, size)
            if product_resource:
                resources.append(product_resource)

        if thumbnail:
            thumbnail_url = thumbnail['href']
            resources.append(self._make_thumbnail_resource(thumbnail_url))
        item['resource'] = resources

        # Add the collection info
        item = self._add_collection(item)

        item['title'] = item['title'].replace('-', '_')

        item['notes'] = item['collection_description']

        item['tags'] = self._get_tags_for_dataset(item)

        return item

    def _make_thumbnail_resource(self, url):
        """
        Return a thumbnail resource dictionary depending on the type.
        """
        name = 'Thumbnail Download'
        description = 'Download the thumbnail.'  # noqa: E501
        _type = 'thumbnail'

        thumbnail = {'name': name,
                        'description': description,
                        'url': url,
                        'format': 'PNG',
                        'mimetype': 'image/png',
                        'resource_type': _type}

        return thumbnail

    def _make_product_resource(self, url, size=None):
        """
        Return a product resource dictionary depending on the product.
        """
        name = 'Product Download'
        description = 'Download the product.' 
        ext = url.split('.')[-1]

        if ext == 'zip':
            mimetype = 'application/zip'
            file_ext = 'ZIP'
            _type = 'zip_product'
        elif 'tif' in ext:
            mimetype = 'image/tiff'
            file_ext = 'GeoTIFF'
            _type = 'tif_product'
        else:
            return None

        product = {'name': name,
                    'description': description,
                    'url': url,
                    'format': file_ext,
                    'mimetype': mimetype,
                    'resource_type': _type}

        if size:
            product['size'] = size

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
