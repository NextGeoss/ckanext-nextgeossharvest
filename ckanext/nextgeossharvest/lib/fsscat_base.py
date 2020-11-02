# -*- coding: utf-8 -*-

import logging
import json
from datetime import timedelta, datetime
import uuid
from ftplib import FTP, error_perm as Ftp5xxErrors
from os import path
from bs4 import BeautifulSoup as Soup
import shapely.geometry
from dateutil.relativedelta import relativedelta

from ckan.model import Session
from ckan.model import Package

from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.model import HarvestObject

from ckanext.nextgeossharvest.lib.fsscat_config import COLLECTION

log = logging.getLogger(__name__)

def parse_filename(url):
    fname = url.split('/')[-1]
    return path.splitext(fname)[0]

def parse_file_extension(url):
    fname = url.split('/')[-1]
    return path.splitext(fname)[1]

def parse_creation_time(date_str):
    date_datetime = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
    return date_datetime.strftime("%Y-%m-%dT%H:%M:%S.000Z")

class FSSCATBase(HarvesterBase):

    def _was_harvested(self, identifier, update_flag):
        """
        Check if a product has already been harvested and return True or False.
        """

        package = Session.query(Package) \
            .filter(Package.name == identifier.lower()).first()

        if package:
            if update_flag:
                log.debug('{} already exists and will be updated.'.format(identifier))  # noqa: E501
                status = 'change'
            else:
                log.debug('{} will not be updated.'.format(identifier))
                status = 'unchanged'
        else:
            log.debug('{} has not been harvested before. Attempting to harvest it.'.format(identifier))  # noqa: E501
            status = 'new'

        return status, package

    def _create_tags(self):
        """Create a list of tags based on the type of harvester."""
        tags_list = [{"name": "FSSCAT"}]

        return tags_list

    def _parse_collection(self, product_type):
        """
        Retrieves the collection information from the configuration
        file, and returns the dictionary with id, name and description
        """
        collection_info = COLLECTION[product_type]
        return collection_info
    
    def _parse_platform_info(self, content):
        """
        Parse the information from the xml regarding the platform.
        Returns the dictionary with the fields gathered
        """
        item = {}
        info = content.find("metadataobject", id="platform")
        #                                               [-180, 90]])
        item['family_name'] = info.find('safe:familyname').text
        instrument_info = info.find('safe:instrument')
        if instrument_info:
            instrument_name = instrument_info.find('safe:familyname')
            item['instrument_name'] = instrument_name.text
        return item

    def _parse_general_product_info(self, content):
        """
        Parse the information from the xml regarding the general
        information of the product.
        Returns the dictionary with the fields gathered
        """
        info = content.find("metadataobject", id="generalInformation")
        normalized_names = {
            "fssp:productname": "identifier",
            "fssp:producttype": "product_type",
            "fssp:class": "class",
            "fssp:baseline": "baseline",
            "fssp:scheme": "scheme",
            "fssp:creationtime": "creation_time"
        }
        item = self._get_elements(normalized_names, info)
        item['identifier'] = parse_filename(item['identifier'])
        creation_time = item.get('creation_time', None)
        if creation_time:
            item['creation_time'] = parse_creation_time(creation_time)

        return item

    def _parse_acquisition_period(self, content):
        """
        Parse the information from the xml regarding the acquisition period.
        Returns the dictionary with the fields gathered
        """
        info = content.find("metadataobject", id="measurementExtent")
        normalized_names = {
            "gml:beginposition": "timerange_start",
            "gml:endposition": "timerange_end"
        }
        item = self._get_elements(normalized_names, info)
        
        for field in item:
            value = item[field]
            date_value = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
            value = datetime.strftime(date_value, "%Y-%m-%dT%H:%M:%S.%fZ")
            item[field] = value
        return item

    def _parse_spatial(self, content):
        """
        Parse the information from the xml regarding the spatial.
        Returns the dictionary with the spatial field
        """
        info = content.find("metadataobject", id="measurementExtent")
        normalized_names = {
            "gmd:westboundlongitude": "west",
            "gmd:eastboundlongitude": "east",
	    "gmd:southboundlatitude": "south",
            "gmd:northboundlatitude": "north"
        }
        tmp_item = self._get_elements(normalized_names, info)
        geojson = self.bbcoords2geometry(float(tmp_item['west']),float(tmp_item['south']),float(tmp_item['east']),float(tmp_item['north']))
        item = {}
        item['spatial'] = json.dumps(geojson)
        return item

    def bbcoords2geometry(self, min_long, min_lat, max_long, max_lat):
	shapely_polygon = shapely.geometry.Polygon([(min_long, min_lat),
                                                    (min_long, max_lat),
                                                    (max_long, max_lat),
                                                    (max_long, min_lat)])
        return json.loads(json.dumps(shapely.geometry.mapping(shapely_polygon)))

    # Required by NextGEOSS base harvester
    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """

        content = json.loads(content)
         
        resources_url = eval(content['resources'])
        name = content['name']
        manifest_content = Soup(content['manifest_content'], 'lxml')

        metadata = {}
        metadata['name'] = name

        metadata.update(self._parse_platform_info(manifest_content))
        metadata.update(self._parse_general_product_info(manifest_content))
        metadata.update(self._parse_acquisition_period(manifest_content))
        metadata.update(self._parse_spatial(manifest_content))
        metadata.update(self._parse_collection(metadata['product_type']))

        metadata['resource'] = self._parse_resources(resources_url, manifest_content)

        metadata['title'] =  metadata['collection_name']
        metadata['notes'] =  metadata['collection_description']
        metadata['tags'] = self._create_tags()

        return metadata

    def _parse_resources(self, resources_url, content):
        """
        Parse the resources of the entry and return a list of dictionaries
        using the CKAN nomenclature.
        """
        
        resources = []
        resources_block = content.find('dataobjectsection')
        
        if resources_block:
            for resource in resources_block.find_all('dataobject'):
                id = resource.get('id', None)
                for url in resources_url:
                    if id in url:
                        mimetype = resource.bytestream.get('mimetype', False)
                        size = resource.bytestream.get('size', False)
                        name = "Product Download"
                        resources.append(self._make_resource(url, name,
                                                             mimetype, size))
                    if "manifest.fssp.safe.xml" in url:
                        mimetype = "application/xml"
                        name = "Metadata Download"
                        resources.append(self._make_resource(url, name,
                                                             mimetype))
        return resources

    def _make_resource(self, url, name, mimetype=None, size=None):
        """
        Create the resource dictionary.
        """
        filename = parse_filename(url)
        extension = parse_file_extension(url).strip('.').upper()
        description_template = ("Download {} from FSSCAT FTP.",
                                "NOTE: DOWNLOAD REQUIRES LOGIN")
        description_template = " ".join(description_template)
        description = description_template.format(filename)
        
        resource = {
            "name": name,
            "description": description,
            "url": url,
            "format": extension
        }
        if size:
            resource["size"] = size
        if mimetype:
            resource["mimetype"] = mimetype

        return resource

    def _get_elements(self, normalized_names, item_node):
        """
        Parse xml block to retrieve the wanted fields and return a
        dictionary containing standard metadata terms and its values
        """
        item = {}
        for subitem_node in item_node.findChildren():
            if subitem_node.name in normalized_names:
                key = normalized_names[subitem_node.name]
                if key and subitem_node.text != '':
                    item[key] = subitem_node.text.strip('\n')
        return item

    # Required by NextGEOSS base harvester
    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        return metadata['resource']

