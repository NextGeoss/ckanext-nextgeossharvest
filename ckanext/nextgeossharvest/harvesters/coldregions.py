# -*- coding: utf-8 -*-
#!/usr/bin/python

"""
---------------------------------------------------------------------
– Project : NextGEOSS
---------------------------------------------------------------------
– Author : João Andrade
– Issue : 0.1
– Date : 13/04/2020
– Purpose : Catalogue the outputs of Cold Regions Pilot
- Source : http://thredds.nersc.no/thredds/catalog/nextgeoss/Svalbard_classification_2018/catalog.xml
---------------------------------------------------------------------
"""

import sys
import requests
import os
import json
import re
import unicodedata
from enum import Enum
import uuid
from bs4 import BeautifulSoup as Soup
import requests


def _create_session():
    """Creates a requests session for NextGEOSS catalogue

    Returns:
        A requests session
    """
    return requests.Session()


def _close_session(session):
    """Closes an active requests MELOA's portal session"""
    session.close()


def _organization_list(session, ckan_url, api_key):
    
    headers = {
        'Cache-Control': 'no-store'
    }
    group_rst = session.get(
        ckan_url + '/api/action/organization_list', headers=headers)

    return group_rst.json()['result']


def _create_organization(name_id, session, ckan_url, api_key):

    if name_id in _organization_list(session, ckan_url, api_key):
        print('Org '+name_id+' already exists. Skipping...')
        return name_id

    params = {
        'name': name_id,
        'title': 'NERSC'
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': api_key
    }
    rsp = session.post(ckan_url + '/api/action/organization_create',
                       json=params,
                       headers=headers)
    if rsp.json()['success'] == True:
        print("Successfully created organization: " + name_id)
        return name_id
    else:
        print(rsp.json())
        return ""


def _create_dataset(package_dict, session, ckan_url, api_key, counter):
    """Creates a dataset on NextGEOSS' ckan catalogue"""

    if counter == 0:
        org_id = _create_organization(package_dict['owner_org'], session, ckan_url, api_key)
        if org_id == "":
            print('Could not create datasets in the catalogue due to organization issues.\n')
            sys.exit()

    #groups = package_dict['groups']

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': api_key
    }
    rsp = session.post(ckan_url + '/api/action/package_create',
                       json=package_dict,
                       headers=headers)
    if rsp.json()['success'] == True:
        print('Successfully created dataset ' + package_dict['name'])
        return True
    else:
        print('Could not create a dataset in the catalogue:\n')
        print(rsp.json())


def upload_to_catalogue(session, files, ckan_url, api_key, owner_org):
    """Uploads the cold region outputs to the catalogue"""

    counter = 0

    for nc_file in files:
        filename = nc_file.split("/")[2]
        product_url = "http://thredds.nersc.no/thredds/fileServer/" + nc_file
        metadata_url = "http://thredds.nersc.no/thredds/dodsC/" + nc_file + ".html"
        getCapabilities = "http://thredds.nersc.no/thredds/wms/" + nc_file + "?service=WMS&version=1.3.0&request=GetCapabilities"

	r = requests.get(getCapabilities)
	soup = Soup(r.content, 'lxml')
	for layer in soup.find_all(queryable = "1"):
	        if layer.find_all("name")[0].text == "ice":
        	        coordinates = layer.find_all("boundingbox")[0]
                	lon_min = coordinates['minx']
	                lon_max = coordinates['maxx']
        	        lat_min = coordinates['miny']
                	lat_max = coordinates['maxy']
	                time = layer.find_all("dimension")[0]['default']

	thumbnail_url = "http://thredds.nersc.no/thredds/wms/" + nc_file + "?service=WMS&version=1.3.0&request=GetMap&layers=ice&CRS=CRS:84&BBOX=" + lon_min + "," + lat_min + "," + lon_max + "," + lat_max + "&width=2048&height=2048&styles=boxfill/ncview&format=image/png&time=" + time

	identifier = "NERSC_ARCTIC_SEAICEEDGE_" + time.replace("-","").replace(":","").replace(".000Z","")

        extras_dict = {'collection_name': 'Sentinel-1 HH/HV based ice/water classification', 'collection_id': 'S1_ARCTIC_SEAICEEDGE_CLASSIFICATION', 'collection_description': 'Sea ice and water classification in the Arctic using Sentinel-1. These datasets have been generated with support from the European Commission in the Horizon 2020 NextGEOSS project (grant agreement No 730329). The algorithms used for Sentinel-1 SAR noise removal and sea ice classification have been developed in the Research Council of Norway project SONARC (Project No 243608) and the Horizon 2020 SPICES project (grant agreement No 640161).', 'spatial': '{"type":"Polygon", "coordinates":[[[' + lon_min + ', ' + lat_max + '], [' + lon_max + ', ' + lat_max + '], [' + lon_max + ', ' + lat_min + '], [' + lon_min + ', ' + lat_min + '], [' + lon_min + ', ' + lat_max + ']]]}', 'timerange_start': time, 'timerange_end': time, 'identifier': identifier, 'filename': filename, 'iso_topic_category': 'oceans', 'institution': 'Nansen Environmental and Remote Sensing Center (NERSC)', 'contributor_name': 'Mohamed Babiker, Anton Korosov, Jeong-Won Park, Torill Hamre, Asuka Yamakawa', 'creator_name': 'Mohamed Babiker', 'contributor_email': 'mohamed.babiker@nersc.no, anton.korosov@nersc.no, jeong-won.park@nersc.no, torill.hamre@nersc.no, asuka.yamakawa@nersc.no', 'source': 'satellite remote sensing', 'satellite': 'Sentinel-1', 'sensor': 'SAR', 'processing_level': 'Level 3', 'groups': [{"name":"cold_regions"}], 'is_output': True}

	package_dict = {}
	package_dict['name'] = extras_dict['identifier'].lower()
	package_dict['title'] = extras_dict['collection_name']
	package_dict['notes'] = extras_dict['collection_description']
	package_dict['tags'] = [{'name': 'earth science'}, {'name': 'ocean'}, {'name': 'sea ice'}]
	if 'groups' in extras_dict:
        	package_dict['groups'] = extras_dict['groups']
	package_dict['extras'] = _get_extras(extras_dict)
	package_dict['resources'] = _set_resources(metadata_url, product_url, thumbnail_url)
	package_dict['private'] = False
	package_dict['id'] = unicode(uuid.uuid4())  # noqa: F821
	package_dict['owner_org'] = owner_org

	_create_dataset(package_dict, session, ckan_url, api_key, counter)
        counter += 1


def _get_extras(extras_dict):
    """Return a list of CKAN extras."""
    skip = {'id', 'title', 'tags', 'status', 'notes', 'name', 'resource', 'groups'}  # noqa: E501
    extras_tmp = [{'key': key, 'value': value} for key, value in extras_dict.items() if key not in skip]
    extras = [{'key': 'dataset_extra', 'value': str(extras_tmp)}]

    return extras


def _set_resources(metadata_url, product_url, thumbnail_url):
        return [{
            'name': 'Metadata on Thredds',
            'url': metadata_url,
            'format': 'html',
            'mimetype': 'text/html'
        }, {
            'name': 'Product Download',
            'description': 'Download the netCDF file',
            'url': product_url,
            'format': 'NetCDF',
            'mimetype': 'application/x-netcdf'
        }, {
            'name': 'Thumbnail Download',
            'url': thumbnail_url,
            'format': 'png',
            'mimetype': 'image/png'
        }]


# MAIN

help_msg = """
                Usage: $ python coldregions.py <data_source_URL> <destination_ckan_URL> <destination_ckan_apikey> <organization_id>
                <data_source_URL> e.g.: "http://thredds.nersc.no/thredds/catalog/nextgeoss/Svalbard_classification_2018/catalog.xml"
                <destination_ckan_URL> e.g.: "http://your-ckan-catalogue"
                <destination_ckan_apikey> : get the API Key of your CKAN admin user"
                <organization_id> e.g.: nersc
        """

if sys.argv[1] == '-h':
	print help_msg
	sys.exit()

try:
        ckan_url = sys.argv[2]
        apikey = sys.argv[3]
        owner_org = sys.argv[4]
	harvest_url = sys.argv[1]
	r = requests.get(harvest_url)
	soup = Soup(r.content, 'lxml')

	files = []
	for dataset in soup.find_all('dataset'):
        	try:
                	files.append(dataset['urlpath'])
	        except Exception:
        	        pass

        if len(files) == 0:
            print "Data Source provided does not have datasets to catalogue.\n"
            sys.exit()

	session = _create_session()
	upload_to_catalogue(session, files, ckan_url, apikey, owner_org)
	_close_session(session)

except Exception:
	print help_msg
