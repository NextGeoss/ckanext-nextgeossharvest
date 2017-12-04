import lxml
import os
import re
import sys
import requests
import time
import uuid
import logging
import shutil

from lxml import etree
from xml.dom import minidom
from requests.auth import HTTPBasicAuth
from string import Template
from datetime import date, timedelta
from datetime import datetime
from bs4 import BeautifulSoup

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.spatial.harvesters.base import SpatialHarvester
from ckan.common import config
from ckan.lib.helpers import json

from bs4 import BeautifulSoup as Soup

log = logging.getLogger(__name__)


class SentinalHarvester(SpatialHarvester):
    extent_template = Template('''
    {"type": "Polygon", "coordinates": [[[$x0, $y0], [$x1, $y1], [$x2, $y2], [$x3, $y3], [$x4, $y4]]]}
    ''')

    def _get_total_datasets(self, url, username, password):
        '''
        Get the total number of datasets, in order to go to the next page
        and get the next entries in the feed
        :return: total number of datasets
        '''
        # user = config.get('ckanext.nextgeossharvest.harvest_username')
        # password = config.get('ckanext.nextgeossharvest.harvest_password')
        #
        # print user
        # print password
        url = str(url) + 'q=*'

        r = requests.get(url, auth=HTTPBasicAuth(username, password), verify=False)
        print r
        if r.status_code != 200:
            print
            'Wrong authentication? status code != 200 - status ' + str(r.status_code)
            return None

        xml = minidom.parseString(r.text)
        total_results = xml.getElementsByTagName("opensearch:totalResults")[0].firstChild.data

        return total_results


    def _get_entries_from_request(self, request):
        '''
        Extract data from the feed
        :return: added id's
        '''
        item = {}
        soup_resp = Soup(request.content, 'xml')
        name_elements = ['str', 'int', 'date', 'double']
        storage_path = config.get('ckan.storage_path')

        for item_node in soup_resp.find_all('entry'):

            for subitem_node in item_node.findChildren():
                key = subitem_node.name
                value = subitem_node.text
                if subitem_node.name in name_elements:
                    key = subitem_node['name']
                item[key] = value

            coordinates_tmp = item['footprint']
            pat = re.compile(r'''(-*\d+\.\d+ -*\d+\.\d+);*''')
            matches = pat.findall(coordinates_tmp)

            if matches:
                lst = [tuple(map(float, m.split())) for m in matches]
                lstlen = len(lst)

                if lstlen == 5:
                    x0 = lst[0][0]
                    y0 = lst[0][1]
                    x1 = lst[1][0]
                    y1 = lst[1][1]
                    x2 = lst[2][0]
                    y2 = lst[2][1]
                    x3 = lst[3][0]
                    y3 = lst[3][1]
                    x4 = lst[4][0]
                    y4 = lst[4][1]

                    coord_values = self.extent_template.substitute(
                        x0=x0, y0=y0, x1=x1, y1=y1, x2=x2, y2=y2, x3=x3, y3=y3, x4=x4, y4=y4
                    )

                    item['spatial'] = coord_values.strip()


            icon_url = item_node.find("link", rel="icon")
            item['thumbnail'] = icon_url['href']

            username = config.get('ckanext.nextgeossharvest.nextgeoss_username')
            password = config.get('ckanext.nextgeossharvest.harvest_password')

            # download the thumbnail and save it in storage_path
            response = requests.get(icon_url['href'],auth=HTTPBasicAuth(username, password), verify=False, stream=True)
            with open(storage_path+'/'+item['uuid']+'.jpg', 'wb') as out_file:
                shutil.copyfileobj(response.raw, out_file)
            del response

            # rel alternative
            alternative = item_node.find("link", rel="alternative")
            item['alternative'] = alternative['href']

            # resource from SciHub
            resource = item_node.find("link")
            item['resource'] = resource['href']

            collection_name = self._set_dataset_name(item['producttype'], item['summary'],
                                                     item['platformname'])
            item['title'] = collection_name['dataset_name']
            item['notes'] = collection_name['notes']

        return item


    def _get_harvest_ids(self, source_url):
        username = config.get('ckanext.nextgeossharvest.nextgeoss_username')
        password = config.get('ckanext.nextgeossharvest.nextgeoss_password')

        r = requests.get(source_url, auth=HTTPBasicAuth(username, password), verify=False)
        if r.status_code != 200:
            print
            'Wrong authentication? status code != 200 - status ' + str(r.status_code)
            return None

        from bs4 import BeautifulSoup as Soup
        soup_resp = Soup(r.content, 'xml')
        uuids = []

        for item_node in soup_resp.find_all('entry'):
            for subitem_node in item_node.findChildren('id'):
                uuids.append(subitem_node.text)

        return uuids


    def _set_dataset_name(self, producttype, summary, platformname):
        '''
        Adds a name for the dataset like collection name.
        Example:  Sentinel-2 Level-1C
        :return: title and description
        '''
        result = {}
        dataset_name = ''
        notes = ''

        if producttype == "RAW":
            dataset_name = "Sentinel-1 Level-0 (Raw)"
            notes = "The Sentinel-1 Level-0 products consist of the sequence of Flexible Dynamic Block Adaptive Quantization (FDBAQ) compressed unfocused SAR raw data. For the data to be usable, it will need to be decompressed and processed using focusing software."
        elif producttype == "SLC":
            dataset_name = "Sentinel-1 Level-1 (SLC)"
            notes = "The Sentinel-1 Level-1 Single Look Complex (SLC) products consist of focused SAR data geo-referenced using orbit and attitude data from the satellite and provided in zero-Doppler slant-range geometry. The products include a single look in each dimension using the full TX signal bandwidth and consist of complex samples preserving the phase information."
        elif producttype == "GRD":
            dataset_name = "Sentinel-1 Level-1 (GRD)"
            notes = "The Sentinel-1 Level-1 Ground Range Detected (GRD) products consist of focused SAR data that has been detected, multi-looked and projected to ground range using an Earth ellipsoid model. Phase information is lost. The resulting product has approximately square resolution pixels and square pixel spacing with reduced speckle at the cost of reduced geometric resolution."
        elif producttype == "OCN":
            dataset_name = "Sentinel-1 Level-2 (OCN)"
            notes = "The Sentinel-1 Level-2 OCN products include components for Ocean Swell spectra (OSW) providing continuity with ERS and ASAR WV and two new components: Ocean Wind Fields (OWI) and Surface Radial Velocities (RVL). The OSW is a two-dimensional ocean surface swell spectrum and includes an estimate of the wind speed and direction per swell spectrum. The OWI is a ground range gridded estimate of the surface wind speed and direction at 10 m above the surface derived from internally generated Level-1 GRD images of SM, IW or EW modes. The RVL is a ground range gridded difference between the measured Level-2 Doppler grid and the Level-1 calculated geometrical Doppler."

        if platformname == 'Sentinel-2':
            dataset_name = "Sentinel-2 Level-1C"
            notes = "The Sentinel-2 Level-1C products are Top-of-atmosphere reflectances in cartographic geometry. These products are systematically generated and the data volume is 500MB for each 100x100" + u"\u33A2" + "."
        elif platformname == 'Sentinel-3':
            dataset_name = "Sentinel-3"
            notes = "SENTINEL-3 is the first Earth Observation Altimetry mission to provide 100% SAR altimetry coverage where LRM is maintained as a back-up operating mode."

        result['dataset_name'] = dataset_name
        result['notes'] = notes + summary

        return result


    def _get_tags_for_dataset(self, item):
        '''
        Creates tags from entry data
        :param item (list of values from the entry)
        :return: tags
        '''
        tags = []
        defined_tags = ('processinglevel', 'platformname', 'instrumentname',
                        'platformserialidentifier', 'producttype', 'processinglevel')

        for key, value in item.items():
            if key in defined_tags:
                tags.append({'name': value})

        return  tags