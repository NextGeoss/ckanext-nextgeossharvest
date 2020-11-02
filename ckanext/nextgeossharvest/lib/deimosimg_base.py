# -*- coding: utf-8 -*-

import logging
import json
from datetime import datetime

from ckanext.harvest.harvesters.base import HarvesterBase

import base64
import os
from string import Template

log = logging.getLogger(__name__)


class DEIMOSIMGBase(HarvesterBase):

    def parse_filename(self, url):
        fname_b64 = url.split('?file=')[1].split('&dir=')[0]
        fname = base64.b64decode(fname_b64)
        return os.path.splitext(fname)[0]

    def parse_filedirectory(self, url):
        fpath_b64 = url.split('&dir=')[1]
        fpath = base64.b64decode(fpath_b64)
        return fpath.split('\\')[1]

    def _get_thumbmail_url(self, identifier, product_type):
        thumbnail_url = ('https://extcat.deimos-imaging.com/extcat4/' +
                         'image2.php?width=150&border=true&imageName={}')
        raw_id = identifier.replace(product_type, 'L0R')
        return thumbnail_url.format(raw_id)

    def _add_tags(self, product_type):
        """Create a list of tags based on the type of harvester."""
        tags_list = [{"name": "deimos2"}]

        if product_type == 'PM4_L1B':
            tags_list.extend([{"name": "panchromatic"},
                              {"name": "l1b"},
                              {"name": "multispectral"}])
        elif product_type == 'PSH_L1B':
            tags_list.extend([{"name": "pan-sharpened"},
                              {"name": "l1b"}])
        elif product_type == 'PSH_L1C':
            tags_list.extend([{"name": "pan-sharpened"},
                              {"name": "l1c"}])

        return tags_list

    def _add_collection(self, item, product_type):
        if product_type == 'PM4_L1B':
            item['collection_id'] = 'DE2_PM4_L1B'
            item['collection_name'] = ('DEIMOS-2 Bundle (Panchromatic' +
                                       ' + Multispectral bands) Level-1B')
            item['collection_description'] = 'DEIMOS-2 PM4-L1B is a five-band image containing the panchromatic and multispectral products packaged together, with band co-registration. The products are calibrated and radiometrically corrected, but not resampled. The geometric information is contained in a rational polynomial.'  # noqa: E501

        elif product_type == 'PSH_L1B':
            item['collection_id'] = 'DE2_PSH_L1B'
            item['collection_name'] = 'DEIMOS-2 Pan-sharpened Level-1B'
            item['collection_description'] = 'DEIMOS-2 PSH-L1B is a four-band image, resulting from adding the information of each multispectral band to the panchromatic band. The products are calibrated and radiometrically corrected, but not resampled. The geometric information is contained in a rational polynomial.'  # noqa: E501

        elif product_type == 'PSH_L1C':
            item['collection_id'] = 'DE2_PSH_L1C'
            item['collection_name'] = 'DEIMOS-2 Pan-sharpened Level-1C'
            item['collection_description'] = 'DEIMOS-2 PSH-L1C is a four-band image, resulting from adding the information of each multispectral band to the panchromatic band. The products are calibrated and radiometrically corrected, manually orthorectified and resampled to a map grid.'  # noqa: E501

        return item

    # Required by NextGEOSS base harvester
    def _parse_content(self, content2parse):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """

        item = json.loads(content2parse)

        starttime_str = item.pop('timerange_start')
        starttime_obj = datetime.strptime(starttime_str, '%Y-%m-%d %H:%M:%S')
        item['timerange_start'] = starttime_obj.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        item['timerange_end'] = item['timerange_start']

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

        ftp_link = item.pop('ftp_link')

        if "http://" in ftp_link:
            ftp_link = ftp_link.replace("http://", "https://")
        elif "https://" in ftp_link or "ftp://" in ftp_link:
            pass
        else:
            ftp_link = "https://" + ftp_link

        product_type = item.pop('product_type')

        item['productType'] = product_type

        resources = []
        resources.append(self._make_resource(ftp_link,
                                             'Product Download'))

        thumbnail_url = self._get_thumbmail_url(item['identifier'],
                                                product_type)
        resources.append(self._make_resource(thumbnail_url,
                                             'Thumbnail Download'))
        item['resource'] = resources

        item['name'] = item['identifier'].lower()

        # Add the collection info
        item = self._add_collection(item, product_type)

        # Add the tag info
        item['tags'] = self._add_tags(product_type)

        item['title'] = item['identifier'].lower()

        item['notes'] = item['collection_description']

        return item

    # Required by NextGEOSS base harvester
    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        return metadata['resource']

    def _make_resource(self, url, name):
        """Return a resource dictionary."""
        resource_dict = {}
        resource_dict['name'] = name
        resource_dict['url'] = url
        if name == 'Thumbnail Download':
            resource_dict['format'] = 'png'
            resource_dict['mimetype'] = 'image/png'
            resource_dict['description'] = ('Download the thumbnail.')
        else:
            resource_dict['format'] = 'zip'
            resource_dict['mimetype'] = 'application/zip'
            resource_dict['description'] = ('Download the product.')

        return resource_dict
