# -*- coding: utf-8 -*-

import logging
import json

from bs4 import BeautifulSoup as Soup

from ckanext.harvest.harvesters.base import HarvesterBase

from string import Template

from ckan.model import Package

from ckan.model import Session

log = logging.getLogger(__name__)


class EBASbaseHarvester(HarvesterBase):

    def _ensure_name_is_unique(self, ideal_name, append_type):
        '''
        Returns a dataset name based on the ideal_name, only it will be
        guaranteed to be different than all the other datasets, by adding a
        number on the end if necessary.

        If generating a new name because the title of the dataset has changed,
        specify the existing name, in case the name doesn't need to change
        after all.

        The maximum dataset name length is taken account of.

        :param ideal_name: the desired name for the dataset, if its not already
                           been taken (usually derived by munging the dataset
                           title)
        :type ideal_name: string

        :param append_type: the type of characters to add to make it unique -
                            either 'number-sequence' or 'random-hex'.
        :type append_type: string
        '''

        if append_type == 'number-sequence':
            MAX_NUMBER_APPENDED = 999999
            PACKAGE_NAME_MAX_LENGTH = 99
            APPEND_MAX_CHARS = len(str(MAX_NUMBER_APPENDED))
        else:
            raise NotImplementedError('append_type cannot be %s' % append_type)
        # Find out which package names have been taken. Restrict it to names
        # derived from the ideal name plus and numbers added
        like_q = u'%s%%' % \
            ideal_name[:PACKAGE_NAME_MAX_LENGTH-APPEND_MAX_CHARS]
        name_results = Session.query(Package.name) \
                              .filter(Package.name.ilike(like_q)) \
                              .all()
        taken = set([name_result[0] for name_result in name_results])
        if ideal_name not in taken:
            # great, the ideal name is available
            return ideal_name
        elif append_type == 'number-sequence':
            # find the next available number
            counter = 1
            while counter <= MAX_NUMBER_APPENDED:
                candidate_name = \
                    ideal_name[:PACKAGE_NAME_MAX_LENGTH - len(str(counter)) - 1] \  # noqa: E501
                    + '_' + str(counter)
                if candidate_name not in taken:
                    return candidate_name
                counter = counter + 1
            return None

    def _normalize_names(self, item_node):
        """
        Return a dictionary of metadata fields with normalized names.

        """

        spatial_dict = {'gmd:westboundlongitude': None,
                        'gmd:eastboundlongitude': None,
                        'gmd:southboundlatitude': None,
                        'gmd:northboundlatitude': None}

        normalized_names = {
            'gmd:abstract': 'notes',
            'gmd:westboundlongitude': 'spatial',
            'gmd:eastboundlongitude': 'spatial',
            'gmd:southboundlatitude': 'spatial',
            'gmd:northboundlatitude': 'spatial',
            'gmd:topiccategory': 'topicCategory',
            'gml:beginposition': 'StartTime',
            'gml:endposition': 'StopTime',
            'gmd:minimumvalue': 'MinimumAltitude',
            'gmd:maximumvalue': 'MaximumAltitude'

        }

        item = {'spatial': spatial_dict}

        for subitem_node in item_node.findChildren():
            if subitem_node.name in normalized_names:
                key = normalized_names[subitem_node.name]
                if key and subitem_node.text != '':
                    value = subitem_node.text.strip('\n')
                    if key is 'spatial':
                        item[key][subitem_node.name] = value
                    else:
                        item[key] = value

        # Since the spatial field is composed by 4 values,
        # if either of the values is None, then the whole field is dismissed
        for key in item['spatial']:
            if item['spatial'][key] is None:
                del item['spatial']
                break

        return item

    def _point_of_contact(self, soup, item):

        normalized_names = {'gmd:individualname': 'PointOfContact',
                            'gmd:organisationname': 'OrganizationName',
                            'gmd:linkage': 'OrganizationLink'}

        for subitem_node in soup.findChildren():
            if subitem_node.name in normalized_names:
                key = normalized_names[subitem_node.name]
                if key and subitem_node.text != '':
                    value = subitem_node.text.strip('\n')
                    item[key] = value

        return item

    def _add_collection(self, item):
        """Return the item with collection ID, name, and description."""

        item['collection_id'] = 'EBAS_NILU_DATA_ARCHIVE'
        item['collection_name'] = 'EBAS NILU Data Archive'  # noqa: E501
        item['collection_description'] = 'Atmospheric composition data obtained at surface in situ stations associated to various international and national frameworks for long-term monitoring. Archived in EBAS database operated by NILU.'  # noqa: E501

        return item

    def _get_tags_for_dataset(self, item):
        """Creates a list of tag dictionaries based on a product's metadata."""
        tags = [{'name': 'EBAS'}]

        return tags

    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        soup = Soup(content, 'lxml')

        # Create an item dictionary and add metadata with normalized names.
        item = {}

        xml_block = soup.find('gmd:md_dataidentification')
        if xml_block:
            item = self._normalize_names(xml_block)

        prod_tagID = 'THREDDS_HTTP_Service'
        xml_block = soup.find('srv:sv_serviceidentification', id=prod_tagID)
        if xml_block:
            url = xml_block.find('srv:connectpoint').find('gmd:linkage').text
            item['product_url'] = url.strip('\n')

        prod_tagID = 'OPeNDAP'
        xml_block = soup.find('srv:sv_serviceidentification', id=prod_tagID)
        if xml_block:
            url = xml_block.find('srv:connectpoint').find('gmd:linkage').text
            item['manifest_url'] = url.strip('\n') + '.html'

        xml_block = soup.find('gmd:contact')
        if xml_block:
            item = self._point_of_contact(xml_block, item)

        xml_block = soup.find('gmd:othercitationdetails')
        if xml_block:
            try:
                json_item = json.loads(xml_block.text.strip('\n'))
                for key in json_item:
                    item[key] = json_item[key]
            except Exception:
                json_item = '1'

        # If there's a spatial element, convert it to GeoJSON
        # Remove it if it's invalid
        spatial_data = item.pop('spatial', None)
        if spatial_data:
            template = Template('''{"type": "Polygon", "coordinates": [$coords_list]}''')  # noqa: E501
            coords_nw = [float(spatial_data['gmd:westboundlongitude']), float(spatial_data['gmd:northboundlatitude'])]  # noqa: E501
            coords_ne = [float(spatial_data['gmd:eastboundlongitude']), float(spatial_data['gmd:northboundlatitude'])]  # noqa: E501
            coords_se = [float(spatial_data['gmd:eastboundlongitude']), float(spatial_data['gmd:southboundlatitude'])]  # noqa: E501
            coords_sw = [float(spatial_data['gmd:westboundlongitude']), float(spatial_data['gmd:southboundlatitude'])]  # noqa: E501
            coords_list = [coords_nw, coords_ne, coords_se, coords_sw, coords_nw]  # noqa: E501

            geojson = template.substitute(coords_list=coords_list)
            item['spatial'] = geojson

        date = item.pop('StartTime', None)
        if date:
            value = date.split('Z')[0]
            value = value + '.000Z'
            item['StartTime'] = value

        date = item.pop('StopTime', None)
        if date:
            value = date.split('Z')[0]
            value = value + '.000Z'
            item['StopTime'] = value

        identifier = soup.find('identifier').text.strip('\n')
        item['identifier'] = identifier

        name = identifier.lower()
        replace_chars = [',', ':', '.', '/', '-']

        for x in replace_chars:
            name = name.replace(x, '_')

        name = name.replace('oai_ebas_oai_pmh_nilu_no_', '')
        name = name[0:42]

        item['name'] = self._ensure_name_is_unique(name, 'number-sequence')

        # Add the collection info
        item = self._add_collection(item)

        item['title'] = item['name'].upper()

        item['tags'] = self._get_tags_for_dataset(item)

        # Add time range metadata that's not tied to product-specific fields
        # like StartTime so that we can filter by a dataset's time range
        # without having to cram other kinds of temporal data into StartTime
        # and StopTime fields, etc.
        item['timerange_start'] = item['StartTime']
        item['timerange_end'] = item['StopTime']

        return item

    def _make_manifest_resource(self, item):
        """
        Return a manifest resource dictionary.
        """
        if item.get('manifest_url'):
            name = 'OPeNDAP html interface'
            description = 'Standard OPeNDAP html interface for selecting data from this dataset.'  # noqa: E501
            url = item.get('manifest_url')
            order = 3
            _type = 'manifest'

            manifest = {'name': name,
                        'description': description,
                        'url': url,
                        'format': 'HTML',
                        'mimetype': 'text/html',
                        'resource_type': _type,
                        'order': order}

            return manifest

        else:
            return None

    def _make_product_resource(self, item):
        """
        Return a product resource dictionary depending on the product.
        """
        if item.get('product_url'):
            name = 'Product Download'
            description = 'Download the product.'  # noqa: E501
            url = item['product_url']
            mimetype = 'application/x-netcdf'
            file_ext = 'NC'
            order = 1
            _type = 'nc_product'

            product = {'name': name,
                       'description': description,
                       'url': url,
                       'format': file_ext,
                       'mimetype': mimetype,
                       'resource_type': _type,
                       'order': order}

            return product
        else:
            return None

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""

        product = self._make_product_resource(parsed_content)
        thumbnail = self._make_manifest_resource(parsed_content)

        resources = [x for x in [product, thumbnail] if x]

        return resources
