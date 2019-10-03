# -*- coding: utf-8 -*-

import logging
import datetime

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
            'endposition': 'StopTime',
            'ingestiondate': 'IngestionDate',
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
            'identifier': 'identifier',
            'size': 'size',
            'platformidentifier': 'PlatformIdentifier',
            'platformserialidentifier': 'PlatformSerialIdentifier',
            'relativeorbitnumber': 'RelativeOrbitNumber',
            'tileid': 'Tileid',
        }
        item = {}

        for subitem_node in item_node.findChildren():
            if subitem_node.name in name_elements:
                key = normalized_names.get(subitem_node.get('name'))
                if key == 'Filename':
                    item['psn'] = subitem_node.text[:3]
                if key:
                    item[key] = subitem_node.text

        return item

    def _add_collection(self, item):
        """Return the item with collection ID, name, and description."""
        identifier = item['identifier'].lower()
        if identifier.startswith('s3'):
            if 'efr' in identifier:
                item['collection_id'] = 'SENTINEL3_OLCI_L1_EFR'
                item['collection_name'] = 'Sentinel-3 OLCI Level-1 Full Resolution'  # noqa: E501
                item['collection_description'] = 'SENTINEL-3 OLCI Level-1 product provides radiances for each pixel in the instrument grid, each view and each OLCI channel, plus annotation data associated to OLCI pixels. The output of this product is during EO processing mode for Full Resolution.'  # noqa: E501
            elif 'err' in identifier:
                item['collection_id'] = 'SENTINEL3_OLCI_L1_ERR'
                item['collection_name'] = 'Sentinel-3 OLCI Level-1 Reduced Resolution'  # noqa: E501
                item['collection_description'] = 'SENTINEL-3 OLCI Level-1 product provides radiances for each pixel in the instrument grid, each view and each OLCI channel, plus annotation data associated to OLCI pixels. The output of this product is during EO processing mode for Reduced Resolution.'  # noqa: E501
            elif 'lfr' in identifier:
                item['collection_id'] = 'SENTINEL3_OLCI_L2_LFR'
                item['collection_name'] = 'Sentinel-3 OLCI Level-2 Land Full Resolution'  # noqa: E501
                item['collection_description'] = 'SENTINEL-3 OLCI level-2 land product provides land and atmospheric geophysical parameters computed for full Resolution.'  # noqa: E501
            elif 'lrr' in identifier:
                item['collection_id'] = 'SENTINEL3_OLCI_L2_LRR'
                item['collection_name'] = 'Sentinel-3 OLCI Level-2 Land Reduced Resolution'  # noqa: E501
                item['collection_description'] = 'SENTINEL-3 OLCI level-2 land product provides land and atmospheric geophysical parameters computed for reduced Resolution.'  # noqa: E501
            elif 'rbt' in identifier:
                item['collection_id'] = 'SENTINEL3_SLSTR_L1_RBT'
                item['collection_name'] = 'Sentinel-3 SLSTR Level-1 Radiances and Brightness Temperatures'  # noqa: E501
                item['collection_description'] = 'SENTINEL-3 SLSTR Level-1 product provides radiances and brightness temperatures for each pixel in a regular image grid, each view and each SLSTR channel, plus annotations data associated with SLSTR pixels.'  # noqa: E501
            elif 'lst' in identifier:
                item['collection_id'] = 'SENTINEL3_SLSTR_L2_LST'
                item['collection_name'] = 'Sentinel-3 SLSTR Level-2 Land Surface Temperature'  # noqa: E501
                item['collection_description'] = 'SENTINEL-3 SLSTR Level-2 LST product provides land surface parameters generated on the wide 1 km measurement grid.'  # noqa: E501
            elif 'cal' in identifier:
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
                message = 'No collection for Sentinel-3 product {}'.format(
                    item["identifier"])
                log.warning(message)
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
        identifier = item['identifier'].lower()
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
                             {'name': 'SAR'}, {'name': 'Altimeter'}])
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

        item['name'] = item['identifier'].lower()

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
            ingestion_date = soup.find('date',
                                       {'name': 'ingestiondate'}).text
            if '.' not in ingestion_date:
                ingestion_date = ingestion_date.replace('Z', '.000Z')
            ingestion_date = datetime.datetime.strptime(ingestion_date,
                                                        '%Y-%m-%dT%H:%M:%S.%fZ')  # noqa: E501
            expiration_date = ingestion_date + datetime.timedelta(days=30)
            item['noa_expiration_date'] = datetime.datetime.strftime(expiration_date, '%Y-%m-%d')  # noqa: E501
        elif enclosure.startswith('https://code-de'):
            item['code_download_url'] = enclosure
            item['code_product_url'] = alternative
            item['code_manifest_url'] = self._make_manifest_url(item)
            if thumbnail:
                item['code_thumbnail'] = thumbnail['href']
        item['thumbnail'] = item.get('scihub_thumbnail') or item.get('noa_thumbnail') or item.get('code_thumbnail')  # noqa: E501

        # Convert size (298.74 MB to an integer representing bytes)
        item['size'] = int(float(item['size'].split(' ')[0]) * 1000000)

        # Add the collection info
        item = self._add_collection(item)

        item['title'] = item['collection_name']

        item['notes'] = item['collection_description']

        # I think this should be the description of the dataset itself
        item['summary'] = soup.find('summary').text

        item['tags'] = self._get_tags_for_dataset(item)

        # Add time range metadata that's not tied to product-specific fields
        # like StartTime so that we can filter by a dataset's time range
        # without having to cram other kinds of temporal data into StartTime
        # and StopTime fields, etc.
        item['timerange_start'] = item['StartTime']
        item['timerange_end'] = item['StopTime']

        return item

    def _make_manifest_url(self, item):
        """Create the URL for manifests on SciHub, NOA, or CODE-DE."""
        if item['name'].startswith('s3'):
            manifest_file = 'xfdumanifest.xml'
        else:
            manifest_file = 'manifest.safe'
        if item.get('scihub_product_url'):
            base_url = 'https://scihub.copernicus.eu/dhus/'
        elif item.get('noa_product_url'):
            base_url = 'https://sentinels.space.noa.gr/dhus/'
        elif item.get('code_product_url'):
            base_url = 'https://code-de.org/dhus/'

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
            name = 'Metadata Download from NOA (valid until {})'.format(item['noa_expiration_date'])  # noqa: E501'
            description = 'Download the metadata manifest from NOA (valid until {}). NOTE: DOWNLOAD REQUIRES LOGIN'.format(item['noa_expiration_date'])  # noqa: E501'
            url = item['noa_manifest_url']
            order = 5
            _type = 'noa_manifest'
        elif item.get('code_manifest_url'):
            name = 'Metadata Download from CODE-DE'
            description = 'Download the metadata manifest from CODE-DE. NOTE: DOWNLOAD REQUIRES LOGIN'  # noqa: E501
            url = item['code_manifest_url']
            order = 6
            _type = 'code_manifest'

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
            name = 'Thumbnail Download from NOA (valid until {})'.format(item['noa_expiration_date'])  # noqa: E501
            description = 'Download the thumbnail from NOA (valid until {}). NOTE: DOWNLOAD REQUIRES LOGIN'.format(item['noa_expiration_date'])  # noqa: E501
            url = item['noa_thumbnail']
            order = 8
            _type = 'noa_thumbnail'
        elif item.get('code_thumbnail'):
            name = 'Thumbnail Download from CODE-DE'
            description = 'Download the thumbnail from CODE-DE.'  # noqa: E501
            url = item['code_thumbnail']
            order = 9
            _type = 'code_thumbnail'
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
            name = 'Product Download from NOA (valid until {})'.format(item['noa_expiration_date'])  # noqa: E501'
            description = 'Download the product from NOA (valid until {}). NOTE: DOWNLOAD REQUIRES LOGIN'.format(item['noa_expiration_date'])  # noqa: E501
            url = item['noa_download_url']
            order = 2
            _type = 'noa_product'
        elif item.get('code_download_url'):
            name = 'Product Download from CODE-DE'
            description = 'Download the product from CODE-DE. NOTE: DOWNLOAD REQUIRES LOGIN'  # noqa: E501
            url = item['code_download_url']
            order = 3
            _type = 'code_product'
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

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        if self.obj.package and self.obj.package.resources:
            old_resources = [x.as_dict() for x in self.obj.package.resources]
        else:
            old_resources = None

        product = self._make_product_resource(parsed_content)
        manifest = self._make_manifest_resource(parsed_content)
        thumbnail = self._make_thumbnail_resource(parsed_content)

        new_resources = [x for x in [product, manifest, thumbnail] if x]
        if not old_resources:
            resources = new_resources
        else:
            # Replace existing resources or add new ones
            new_resource_types = {x['resource_type'] for x in new_resources}
            resources = []
            for old in old_resources:
                old_type = old.get('resource_type')
                order = old.get('order')
                if old_type not in new_resource_types and order:
                    resources.append(old)
            resources += new_resources

            resources.sort(key=lambda x: x['order'])

        return resources
