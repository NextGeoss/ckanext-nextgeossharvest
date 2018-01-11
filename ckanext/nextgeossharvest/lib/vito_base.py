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

class BaseError(Exception):
    pass



class VitoProbaVHarvester(SpatialHarvester):
    '''
    Preform various operations on a Vito
    '''
    extent_template = Template('''
    {"type": "Polygon", "coordinates": [[[$x0, $y0], [$x1, $y1], [$x2, $y2], [$x3, $y3], [$x4, $y4]]]}
    ''')


    def _get_collections(self, url, collectionurl, platform):
        '''
        Get the collections from VITO that are under PLATFORM=platform
        :param url: url to VITO OpenSearch
        :param platform: name of the platform that we want to collect from
        :return:
        '''
        url_collections = url + collectionurl + '?platform=' + platform
        request = requests.get(url_collections)
        paramenters = ['id', 'title', 'summary']
        collections = []

        if request.status_code != 200:
            print
            'Wrong authentication? status code != 200 - status ' + str(request.status_code)
            return None

        soup_resp = Soup(request.content, 'xml')
        for item_node in soup_resp.find_all('entry'):
            tmp = {}
            for subitem_node in item_node.findChildren():
                if subitem_node.name in paramenters:
                    key = subitem_node.name
                    value = subitem_node.text
                    tmp[key] = value
            collections.append(tmp)

        return collections


    def _get_total_datasets(self, url):
        '''
        Get the total number of datasets, in order to go to the next page
        and get the next entries in the feed
        :return: total number of datasets
        '''

        r = requests.get(url)
        print r
        if r.status_code != 200:
            print
            'Wrong authentication? status code != 200 - status ' + str(r.status_code)
            return None

        soup_resp = Soup(r.content, 'xml')
        total_results = soup_resp.totalResults.text

        return total_results


    def _get_harvest_ids(self, request):
        soup_resp = Soup(request.content, 'xml')
        uuids = []

        for item_node in soup_resp.find_all('entry'):
            for subitem_node in item_node.findChildren('id'):
                uuids.append(subitem_node.text)

        return uuids


    def _get_missing_harvest_ids(self, request, missing_datatasets):
        soup_resp = Soup(request.content, 'xml')
        uuids = []
        count = 0

        while count < 0:
            for item_node in soup_resp.find_all('entry'):
                for subitem_node in item_node.findChildren('id'):
                    uuids.append(subitem_node.text)
                count +=1

        return uuids


    def _get_last_page(self, url):
        request = requests.get(url)
        soup_resp = Soup(request.content, 'xml')
        last_page_tmp = soup_resp.find("link", rel="last")
        last_page = last_page_tmp['href']

        return last_page


    def _get_previous_page(self, request):
        soup_resp = Soup(request.content, 'xml')
        prev_page_tmp = soup_resp.find("link", rel="previous")
        prev_page = prev_page_tmp['href']

        return prev_page


    def _get_items_per_page(self, request):
        soup_resp = Soup(request.content, 'xml')
        items_per_page = soup_resp.itemsPerPage.text
        return  items_per_page


    def _get_entries_from_request(self, request):
        '''
        Extract data from the feed
        :return: added id's
        '''
        item = {}
        thumbnails_storage_path = config.get('ckan.thumbnails_storage_path')

        soup_resp = Soup(request.content, 'xml')
        skip_elements = ['group', 'content', 'category', 'EarthObservation', 'phenomenonTime', 'TimePeriod', 'resultTime'
                         'TimeInstant', 'procedure', 'EarthObservationEquipment', 'shortName', 'Sensor', 'acquisitionParameters'
                         'Acquisition', 'observedProperty', 'featureOfInterest', 'Footprint', 'multiExtentOf', 'MultiSurface',
                         'surfaceMembers', 'exterior', 'LinearRing', 'posList', 'centerOf', 'EarthObservationResult',
                         'browse', 'BrowseInformation', 'type', 'fileName', 'RequestMessage', 'ServiceReference',
                         'EarthObservationMetaData', 'SpecificInformation', 'metaDataProperty', 'vendorSpecific', 'result',
                         'Sensor', 'localAttribute', 'link']


        for item_node in soup_resp.find_all('entry'):
            for subitem_node in item_node.findChildren():
                key = subitem_node.name
                if key not in skip_elements:
                    value = subitem_node.text
                    if subitem_node.key is not None:
                        value = subitem_node.key.text
                    if key not in item:
                        item[key.lower()] = value

            coordinates_tmp = item['polygon']
            pat = re.compile(r'''(-*\d+\.\d+ -*\d+\.\d+);*''')
            matches = pat.findall(coordinates_tmp)

            if matches:
                lst = [tuple(map(float, m.split())) for m in matches]
                lstlen = len(lst)

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

            icon_url = item_node.find("content", type="image/png")
            item['thumbanil'] = icon_url['url']

            # download the thumbnail and save it in storage_path
            response = requests.get(icon_url['url'], stream=True)
            with open(thumbnails_storage_path + '/' + item['identifier'] + '.png', 'wb') as out_file:
                shutil.copyfileobj(response.raw, out_file)
            del response

            # resource
            resource_tmp = item_node.find("link", type="application/metalink+xml")
            if resource_tmp is None:
                resource_tmp = item_node.find("link", type="application/octet-stream")

            item['resource'] = resource_tmp['href']
            item['collection_name'] = item['parentidentifier']

        return item


    def _get_dataset_description(self, dataset_name):
        '''
        Adds a name for the dataset like collection name (producttype in metadata).
        Example:  Proba-V L2A 333M
        :return: description
        '''
        result = {}
        notes = ''

        if dataset_name == "PROBAV_L3_S1_TOC_1KM":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of" \
                    " 1 day for 1Km of spatial resolution."

        elif dataset_name == "PROBAV_L3_S1_TOA_1KM":
            notes = "Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame " \
                    "of 1 day for 1Km of spatial resolution."

        elif dataset_name == "PROBAV_L3_S10_TOC_1KM":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of " \
                    "10 days for 1Km of spatial resolution."

        elif dataset_name == "PROBAV_L3_S10_TOC_NDVI_1KM":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of " \
                    "10 days for 1Km of spatial resolution, containing only Normalized Difference Vegetation Index (NDVI)."

        elif dataset_name == "ProbaV L2A - 1km":
            notes = "PROBA-V Level2A - 1KM segments contain the Level 1C (P product) data projected on a uniform " \
                    "1Km grid."

        elif dataset_name == "PROBAV_L1C":
            notes = "Raw data which is geo-located and radiometrically calibrated to Top Of Atmosphere (TOA) " \
                    "reflectance values."

        elif dataset_name == "PROBAV_L3_S10_TOC_333M":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of " \
                    "10 days for 333m of spatial resolution."

        elif dataset_name == "PROBAV_L3_S1_TOC_333M":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of " \
                    "1 day for 333m of spatial resolution."

        elif dataset_name == "PROBAV_L3_S1_TOA_333M":
            notes = "Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame " \
                    "of 1 day for 333m of spatial resolution."

        elif dataset_name == "ProbaV S10 TOC - 333 m":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of " \
                    "10 days for 333m of spatial resolution."

        elif dataset_name == "PROBAV_L3_S10_TOC_NDVI_333M":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of " \
                    "10 days for 333m of spatial resolution, containing only Normalized Difference Vegetation Index (NDVI)."

        elif dataset_name == "ProbaV L2A - 333 m":
            notes = "PROBA-V Level2A - 333M segments contain the Level 2C (P product) data projected on a uniform " \
                    "333m grid."

        elif dataset_name == "PROBAV_L3_S1_TOC_100M":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of " \
                    "1 day for 100m of spatial resolution."

        elif dataset_name == "PROBAV_L3_S1_TOA_100M":
            notes = "Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame " \
                    "of 1 day for 100m of spatial resolution."

        elif dataset_name == "PROBAV_L3_S1_TOC_NDVI_100M":
            notes = "Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame " \
                    "of 1 day for 100m of spatial resolution, containing only Normalized Difference Vegetation Index (NDVI)."

        elif dataset_name == "PROBAV_L3_S5_TOC_100M":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of " \
                    "5 days for 100m of spatial resolution."

        elif dataset_name == "PROBAV_L3_S5_TOA_100M":
            notes = "Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame of " \
                    "5 days for 100m of spatial resolution."

        elif dataset_name == "PROBAV_L3_S5_TOC_NDVI_100M":
            notes = "Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame of " \
                    "5 days for 100m of spatial resolution, containing only Normalized Difference Vegetation Index (NDVI)."

        elif dataset_name == "PROBAV_L2A_100M":
            notes = "PROBA-V Level2A - 100M segments contain the Level 1C (P product) data projected on a uniform " + \
                    "100m grid."

        elif dataset_name == "PROBAV_L2A_1KM":
            notes = "PROBA-V Level2A - 1KM segments contain the Level 1C (P product) data projected on a uniform " \
                    "1km grid."

        elif dataset_name == "PROBAV_L2A_333M":
            notes = "PROBA-V Level2A - 333M segments contain the Level 1C (P product) data projected on a uniform " \
                    "333m grid."

        return notes


    def _get_tags_for_dataset(self, item):
        '''
        Creates tags from entry data
        :param item (list of values from the entry)
        :return: tags
        '''
        tags = []
        defined_tags = ('instrument', 'platform')

        for key, value in item.items():
            if key in defined_tags:
                tags.append({'name': value})
            if key == 'productType':
                value_tmp = value.split("_")
                for val in value_tmp:
                    tags.append({"name": val})

        return  tags