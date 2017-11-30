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

log = logging.getLogger(__name__)


class SentinalHarvester(SpatialHarvester):
    def get_total_datasets(self, config, url):
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

    def parse_save_entry_data(self, config, url, obj, total_results):
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

                for k, v in item.items():
                    obj.extras.append(HOExtra(key=k, value=v))
                ids.append(obj.id)
            #start += 1

        print 'Finished creating HarvestObjectExtras for ESAHarvest'
        return ids