# -*- coding: utf-8 -*-

import logging

from bs4 import BeautifulSoup as Soup

from ckanext.harvest.harvesters.base import HarvesterBase


log = logging.getLogger(__name__)


class SentinelHarvester(HarvesterBase):

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
        name_elements = ['str', 'int', 'date', 'double']
        normalized_names = {
            'beginposition': 'StartTime',
            'endposition': 'EndTime',
            'footprint': 'spatial',
            'filename': 'Filename',
            'platformname': 'FamilyName',
            'instrumentshortname': 'InstrumentFamilyName',
            'instrumentname': 'InstrumentName',
            'polarisationmode': 'TransmitterReceiverPolarisation',
            'sensoroperationalmode': 'InstrumentMode',
            'productclass': 'ProductClass',
            'producttype': 'ProductType',
            'productconsolidation': 'ProductConsolidation',
            'acquisitiontype': 'AcquisitionType',
            'orbitdirection': 'OrbitDirection',
            'swathidentifier': 'Swath',
            'cloudcoverpercentage': 'CloudCoverage',
            'uuid': 'uuid',
            'identifier': 'Identifier',
            'size': 'size',
        }
        item = {}

        for subitem_node in item_node.findChildren():
            key = subitem_node.name
            value = subitem_node.text
            if subitem_node.name in name_elements:
                key = normalized_names.get(subitem_node['name'])
                if key:
                    item[key] = value

        return item

    def _add_collection(self, item):
        """Return the item with collection ID, name, and description."""
        identifier = item['Identifier'].lower()
        if identifier.startswith('s3'):
            if 'cal' in identifier:
                item['collection_id'] = 'SENTINEL3_SRAL_L1_CAL'
                item['collection_name'] = 'Sentinel-3 SRAL Level-1 Calibration'
                item['collection_description'] = 'SENTINEL-3 is the first Earth Observation Altimetry mission to provide 100% SAR altimetry coverage where LRM is maintained as a back-up operating mode. This is a Level 1 Calibration product.'  # noqa: E501
            elif 'sra' in identifier:
                item['collection_id'] = 'SENTINEL3_SRAL_L1_SRA'
                item['collection_name'] = 'Sentinel-3 SRAL Level-1 SRA'
                item['collection_description'] = 'SENTINEL-3 is the first Earth Observation Altimetry mission to provide 100% SAR altimetry coverage where LRM is maintained as a back-up operating mode. This is a Level 1 SRA product.'  # noqa: E501
            elif 'lan' in identifier:
                item['collection_id'] = 'SENTINEL3_SRAL_L2_LAN'
                item['collection_name'] = 'Sentinel-3 SRAL Level-2 Land'
                item['collection_description'] = 'SENTINEL-3 is the first Earth Observation Altimetry mission to provide 100% SAR altimetry coverage where LRM is maintained as a back-up operating mode. This is a product of Level 2 processing and geographical coverage over land.'  # noqa: E501
            elif 'wat' in identifier:
                item['collection_id'] = 'SENTINEL3_SRAL_L2_WAT'
                item['collection_name'] = 'Sentinel-3 SRAL Level-2 Water'
                item['collection_description'] = 'SENTINEL-3 is the first Earth Observation Altimetry mission to provide 100% SAR altimetry coverage where LRM is maintained as a back-up operating mode. This is a product of Level 2 processing and geographical coverage over water.'  # noqa: E501
            else:
                log.warning('No collection for Sentinel-3 product {}'.format(identifier))  # noqa: E501
        elif identifier.startswith('s2'):
            if 'msil1c' in identifier:
                item['collection_id'] = 'SENTINEL2_L1C'
                item['collection_name'] = 'Sentinel-2 Level-1C'
                item['collection_description'] = u'The Sentinel-2 Level-1C products are Top-of-atmosphere reflectances in cartographic geometry. These products are systematically generated and the data volume is 500MB for each 100x100 km².'  # noqa: E501
            elif 'msil2a' in identifier:
                item['collection_id'] = 'SENTINEL2_L2A'
                item['collection_name'] = 'Sentinel-2 Level-2A'
                item['collection_description'] = u'The Sentinel-2 Level-2A products are Bottom-of-atmosphere reflectances in cartographic geometry (prototype product). These products are generated using Sentinel-2 Toolbox and the data volume is 600MB for each 100x100 km².'  # noqa: E501

            else:
                log.warning('No collection for Sentinel-2 product {}'.format(identifier))  # noqa: E501
        else:
            if 'slc' in identifier:
                item['collection_id'] = 'SENTINEL1_L1_SLC'
                item['collection_name'] = 'Sentinel-1 Level-1 (SLC)'
                item['collection_description'] = 'The Sentinel-1 Level-1 Single Look Complex (SLC) products consist of focused SAR data geo-referenced using orbit and attitude data from the satellite and provided in zero-Doppler slant-range geometry. The products include a single look in each dimension using the full TX signal bandwidth and consist of complex samples preserving the phase information.'  # noqa: E501
            elif 'grd' in identifier:
                item['collection_id'] = 'SENTINEL1_L1_GRD'
                item['collection_name'] = 'Sentinel-1 Level-1 (GRD)'
                item['collection_description'] = 'The Sentinel-1 Level-1 Ground Range Detected (GRD) products consist of focused SAR data that has been detected, multi-looked and projected to ground range using an Earth ellipsoid model. Phase information is lost. The resulting product has approximately square resolution pixels and square pixel spacing with reduced speckle at the cost of reduced geometric resolution.'  # noqa: E501
            elif 'ocn' in identifier:
                item['collection_id'] = 'SENTINEL1_L2_OCN'
                item['collection_name'] = 'Sentinel-1 Level-2 (OCN)'
                item['collection_description'] = 'The Sentinel-1 Level-2 OCN products include components for Ocean Swell spectra (OSW) providing continuity with ERS and ASAR WV and two new components: Ocean Wind Fields (OWI) and Surface Radial Velocities (RVL). The OSW is a two-dimensional ocean surface swell spectrum and includes an estimate of the wind speed and direction per swell spectrum. The OWI is a ground range gridded estimate of the surface wind speed and direction at 10 m above the surface derived from internally generated Level-1 GRD images of SM, IW or EW modes. The RVL is a ground range gridded difference between the measured Level-2 Doppler grid and the Level-1 calculated geometrical Doppler.'  # noqa: E501
            elif 'raw' in identifier:
                item['collection_id'] = 'SENTINEL1_L0_RAW'
                item['collection_name'] = 'Sentinel-1 Level-0 (RAW)'
                item['collection_description'] = 'The Sentinel-1 Level-0 products consist of the sequence of Flexible Dynamic Block Adaptive Quantization (FDBAQ) compressed unfocused SAR raw data. For the data to be usable, it will need to be decompressed and processed using focusing software.'  # noqa: E501
            else:
                log.warning('No collection for Sentinel-1 product {}'.format(identifier))  # noqa: E501

        return item

    def _get_tags_for_dataset(self, item):
        """Creates a list of tag dictionaries based on a product's metadata."""
        identifier = item['Identifier'].lower()
        if identifier.startswith('s1'):
            tags = [{'name': 'Sentinel-1'}]
            if 'slc' in identifier:
                tags.extend([{'name': 'SLC'}])
            elif 'grd' in identifier:
                tags.extend([{'name': 'GRD'}])
            elif 'ocn' in identifier:
                tags.extend([{'name': 'OCN'}])
            elif 'raw' in identifier:
                tags.extend([{'name': 'RAW'}])
        elif identifier.startswith('s2'):
            tags = [{'name': 'Sentinel-2'}, {'name': 'MSI'}]
            if 'msil1c' in identifier:
                tags.extend([{'name': 'Level-1C'}])
            elif 'msil2a' in identifier:
                tags.extend([{'name': 'Level-2A'}])
        elif identifier.startswith('s3'):
            tags = [{'name': 'Sentinel-3'}]
            if 'lan' in identifier:
                tags.extend([{'name': 'Land'}, {'name': 'Level-2'},
                             {'name': 'SAR'}, {'name': 'Altimeter'}])
            elif 'wat' in identifier:
                tags.extend([{'name': 'Water'}, {'name': 'Level-2'},
                             {'name': 'SAR'}, {'name': 'Altimete'}])
            elif 'sra' in identifier:
                tags.extend([{'name': 'SRA'}, {'name': 'Level-1'},
                             {'name': 'SAR'}, {'name': 'Altimeter'}])
            elif 'cal' in identifier:
                tags.extend([{'name': 'Calibration'}, {'name': 'Level-1'},
                             {'name': 'SAR'}, {'name': 'Altimeter'}])
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
        geojson = self._convert_to_geojson(item.pop('spatial', None))
        if geojson:
            item['spatial'] = geojson

        item['name'] = item['Identifier'].lower()

        # Thumbnail, alternative and enclosure
        enclosure = soup.find('link', rel=None)['href']
        alternative = soup.find('link', rel='alternative')['href']
        thumbnail = soup.find('link', rel='icon')
        if enclosure.startswith('https://scihub'):
            item['scihub_download_url'] = enclosure
            item['scihub_product_url'] = alternative
            item['scihub_manifest_url'] = self._make_manifest_url(item)
            if thumbnail:
                item['scihub_thumbnail'] = thumbnail['href']
        elif enclosure.startswith('https://sentinels'):
            item['noa_download_url'] = enclosure
            item['noa_product_url'] = alternative
            item['noa_manifest_url'] = self._make_manifest_url(item)
            if thumbnail:
                item['noa_thumbnail'] = thumbnail['href']
        item['thumbnail'] = item.get('scihub_thumbnail') or item.get('noa_thumbnail')  # noqa: E501

        # Convert size (298.74 MB to an integer representing bytes)
        item['size'] = int(float(item['size'].split(' ')[0]) * 1000000)

        # Add the collection info
        item = self._add_collection(item)

        item['title'] = item['collection_name']

        item['notes'] = item['collection_description']

        # I think this should be the description of the dataset itself
        item['summary'] = soup.find('summary').text

        item['tags'] = self._get_tags_for_dataset(item)

        return item

    def _update_tags(self, old_tags, new_tags):
        """
        Add any new tags from the harvester, but preserve existing tags
        so that we don't lose tags from iTag or from other harvesters.
        """
        old_tag_names = {tag['name'] for tag in old_tags}
        for tag in new_tags:
            if tag['name'] not in old_tag_names:
                old_tags.append(tag)

        return old_tags

    def _update_extras(self, old_extras, new_extras):
        """
        Add new extras from the harvester, but preserve existing extras
        so that we don't lose any from iTag.

        In the future, we should restrict the filter to only extras from iTag
        so that we can update metadata names more easily. We don't know what
        we'll get from iTag, though, so that's off the table for now.
        """
        old_extra_keys = {old_extra['key'] for old_extra in old_extras}
        for new_extra in new_extras:
            if new_extra['key'] not in old_extra_keys:
                old_extras.append(new_extra)

        return old_extras

    def _make_manifest_url(self, item):
        """Create the URL for manifests on SciHub or NOA."""
        if item['name'].startswith('s3'):
            manifest_file = 'xfdumanifest.xml'
        else:
            manifest_file = 'manifest.safe'
        if item.get('scihub_product_url'):
            base_url = 'https://scihub.copernicus.eu/apihub/'
        elif item.get('noa_product_url'):
            base_url = 'https://sentinels.space.noa.gr/dhus/'

        manifest_url = "{}odata/v1/Products('{}')/Nodes('{}')/Nodes('{}')/$value".format(base_url, item['uuid'], item['Filename'], manifest_file)  # noqa: E501

        return manifest_url

    def _make_manifest_resource(self, item):
        """
        Return a manifest resource dictionary depending on the harvest source.
        """
        if item.get('scihub_manifest_url'):
            name = 'Metadata Download from SciHub'
            description = 'Download the metadata manifest from SciHub. NOTE: DOWNLOAD REQUIRES LOGIN'  # noqa: E501
            url = item['scihub_manifest_url']
            order = 4
            _type = 'scihub_manifest'
        elif item.get('noa_manifest_url'):
            name = 'Metadata Download from NOA'
            description = 'Download the metadata manifest from NOA. NOTE: DOWNLOAD REQUIRES LOGIN'  # noqa: E501
            url = item['noa_manifest_url']
            order = 5
            _type = 'noa_manifest'
        else:
            return None
            # It's not clear where the CODE-DE manifests are, so return None
            # for now. We can update later.

        manifest = {'name': name,
                    'description': description,
                    'url': url,
                    'format': 'XML',
                    'mimetype': 'application/xml',
                    'resource_type': _type,
                    'order': order}

        return manifest

    def _make_thumbnail_resource(self, item):
        """
        Return a thumbnail resource dictionary depending on the harvest source.
        """
        if item.get('scihub_thumbnail'):
            name = 'Thumbnail Download from SciHub'
            description = 'Download the thumbnail from SciHub. NOTE: DOWNLOAD REQUIRES LOGIN'  # noqa: E501
            url = item['scihub_thumbnail']
            order = 7
            _type = 'scihub_thumbnail'
        elif item.get('noa_thumbnail'):
            name = 'Thumbnail Download from NOA'
            description = 'Download the thumbnail from NOA. NOTE: DOWNLOAD REQUIRES LOGIN'  # noqa: E501
            url = item['noa_thumbnail']
            order = 8
            _type = 'noa_thumbnail'
        elif item.get('codede_thumbnail'):
            name = 'Thumbnail Download from CODE-DE'
            description = 'Download the thumbnail from CODE-DE.'  # noqa: E501
            url = item['codede_thumbnail']
            order = 9
            _type = 'codede_thumbnail'
        else:
            return None

        thumbnail = {'name': name,
                     'description': description,
                     'url': url,
                     'format': 'JPEG',
                     'mimetype': 'image/jpeg',
                     'resource_type': _type,
                     'order': order}

        return thumbnail

    def _make_product_resource(self, item):
        """
        Return a product resource dictionary depending on the harvest source.
        """
        if item.get('scihub_download_url'):
            name = 'Product Download from SciHub'
            description = 'Download the product from SciHub. NOTE: DOWNLOAD REQUIRES LOGIN'  # noqa: E501
            url = item['scihub_download_url']
            order = 1
            _type = 'scihub_product'
        elif item.get('noa_download_url'):
            name = 'Product Download from NOA'
            description = 'Download the product from NOA. NOTE: DOWNLOAD REQUIRES LOGIN'  # noqa: E501
            url = item['noa_download_url']
            order = 2
            _type = 'noa_product'
        elif item.get('codede_download_url'):
            name = 'Product Download from CODE-DE'
            description = 'Download the product from CODE-DE.'  # noqa: E501
            url = item['codede_download_url']
            order = 3
            _type = 'codede_product'
        size = item['size']

        product = {'name': name,
                   'description': description,
                   'url': url,
                   'format': 'SAFE',
                   'mimetype': 'application/zip',
                   'size': size,
                   'resource_type': _type,
                   'order': order}

        return product
