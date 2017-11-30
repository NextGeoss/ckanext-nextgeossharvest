import lxml
import os
import re
import sys
import requests
import time
import uuid
import logging

from lxml import etree
from xml.dom import minidom
from requests.auth import HTTPBasicAuth
from datetime import date, timedelta
from datetime import datetime
from bs4 import BeautifulSoup

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.spatial.harvesters.base import SpatialHarvester

from bs4 import BeautifulSoup as Soup

log = logging.getLogger(__name__)


class SentinalHarvester(SpatialHarvester):
    def _get_total_datasets(self, config, url):
        '''
        Get the total number of datasets, in order to go to the next page
        and get the next entries in the feed
        :return: total number of datasets
        '''
        #user = config.get('username')
        #password = config.get('password')
        user = 'nextgeoss'
        password = 'nextgeoss'

        r = requests.get(url, auth=HTTPBasicAuth(user, password), verify=False)
        if r.status_code != 200:
            print
            'Wrong authentication? status code != 200 - status ' + str(r.status_code)
            return None

        xml = minidom.parseString(r.text)
        total_results = xml.getElementsByTagName("opensearch:totalResults")[0].firstChild.data

        return total_results


    def parse_save_entry_data(self, config, source_url, guid, harvest_job, total_results):
        '''
        This function gets the data from the feed and creates
        a HarvestObject
        :return: added id's
        '''
        start = 0
        user = 'nextgeoss'
        password = 'nextgeoss'
        ids = []

        #if start < total_results:
        r = requests.get(source_url, auth=HTTPBasicAuth(user, password), verify=False)
        if r.status_code != 200:
            print
            'Wrong authentication? status code != 200 - status ' + str(r.status_code)
            return None

        soup_resp = Soup(r.content, 'xml')

        name_elements = ['str', 'int', 'date', 'double']

        for item_node in soup_resp.find_all('entry'):
            item = {}

            obj = HarvestObject(guid=guid, job=harvest_job,
                                extras=[HOExtra(key='status', value='new')])
            obj.save()

            for subitem_node in item_node.findChildren():
                key = subitem_node.name
                value = subitem_node.text
                if subitem_node.name in name_elements:
                    key = subitem_node['name']
                item[key] = value

                if subitem_node.name in 'link':
                    key = subitem_node['href']
                    continue

            log.debug('Create ESA HarvestObjectExtra for %s',
                      item['id'])

            if item['producttype']:
                collection_name = self._set_dataset_name(item['producttype'])
                item['title'] = collection_name['dataset_name']
                item['notes'] = collection_name['notes']

            for k, v in item.items():
                obj.extras.append(HOExtra(key=k, value=v))
            ids.append(obj.id)
            #start += 1

        print 'Finished creating HarvestObjectExtras for ESAHarvest'
        return ids


    def _get_harvest_ids(self, source_url):
        user = 'nextgeoss'
        password = 'nextgeoss'
        r = requests.get(source_url, auth=HTTPBasicAuth(user, password), verify=False)
        if r.status_code != 200:
            print
            'Wrong authentication? status code != 200 - status ' + str(r.status_code)
            return None

        from bs4 import BeautifulSoup as Soup
        soup_resp = Soup(r.content, 'xml')
        uuids = []

        for item_node in soup_resp.find_all('entry'):
            item = {}
            for subitem_node in item_node.findChildren('id'):
                uuids.append(subitem_node.text)

        return uuids


    def _set_dataset_name(self, producttype):
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

        result['dataset_name'] = dataset_name
        result['notes'] = notes

        return result