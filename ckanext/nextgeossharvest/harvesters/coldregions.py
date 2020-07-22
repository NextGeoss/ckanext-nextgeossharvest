# -*- coding: utf-8 -*-
#!/usr/bin/python

"""
---------------------------------------------------------------------
– Project : NextGEOSS
---------------------------------------------------------------------
– Author : João Andrade
– Issue : 0.1
– Date : 22/04/2020
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
from datetime import datetime
import shlex


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
        'Content-Type': 'application/json',
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
        'Content-Type': 'application/json',
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


def upload_to_catalogue(session, files, ckan_url, api_key, owner_org, collection):
    """Uploads the cold region outputs to the catalogue"""

    counter = 0

    for nc_file in files:
        filename = nc_file.split("/")[2]
        product_url = "http://thredds.nersc.no/thredds/fileServer/" + nc_file
        metadata_url = "http://thredds.nersc.no/thredds/dodsC/" + nc_file + ".html"
        getCapabilities = "http://thredds.nersc.no/thredds/wms/" + nc_file + "?service=WMS&version=1.3.0&request=GetCapabilities"

        r = requests.get(getCapabilities)
        soup = Soup(r.content, 'lxml')

        date_format_orig = "%Y%m%dT%H%M%S"
        date_format_new = "%Y-%m-%dT%H:%M:%S.000Z"

        for layer in soup.find_all(queryable = "1"):
            if layer.find_all("name")[0].text == "ice" or layer.find_all("name")[0].text == "ice_water":
                coordinates = layer.find_all("boundingbox")[0]
                lon_min = coordinates['minx']
                lon_max = coordinates['maxx']
                lat_min = coordinates['miny']
                lat_max = coordinates['maxy']
                if layer.find_all("name")[0].text == "ice":
	            start_time = layer.find_all("dimension")[0]['default']
                    stop_time = start_time
                    identifier = "NERSC_ARCTIC_SEAICEEDGE_" + start_time.replace("-","").replace(":","").replace(".000Z","")
                    layer_name = "ice"
                    contributor_name = "Mohamed Babiker, Anton Korosov, Jeong-Won Park, Torill Hamre, Asuka Yamakawa"
                    creator_name = "Mohamed Babiker"
                    contributor_email = "mohamed.babiker@nersc.no, anton.korosov@nersc.no, jeong-won.park@nersc.no, torill.hamre@nersc.no, asuka.yamakawa@nersc.no"
                elif layer.find_all("name")[0].text == "ice_water":
                    start_time = filename.split('_')[4]
                    identifier = "NERSC_ARCTIC_SEAICEEDGE_" + start_time
                    start_time = datetime.strptime(start_time, date_format_orig).strftime(date_format_new)
                    stop_time = filename.split('_')[5]
                    stop_time = datetime.strptime(stop_time, date_format_orig).strftime(date_format_new)
                    layer_name = "ice_water"
                    contributor_name = "Frode Monsen, Torill Hamre, Mohamed Babiker, Anton Korosov, Jeong-Won Park"
                    creator_name = "Frode Monsen"
                    contributor_email = "frode.monsen@nersc.no, torill.hamre@nersc.no, mohamed.babiker@nersc.no, anton.korosov@nersc.no, jeong-won.park@nersc.no"

                thumbnail_url = "http://thredds.nersc.no/thredds/wms/" + nc_file + "?service=WMS&version=1.3.0&request=GetMap&layers=" + layer_name + "&CRS=CRS:84&BBOX=" + lon_min + "," + lat_min + "," + lon_max + "," + lat_max + "&width=2048&height=2048&styles=boxfill/ncview&format=image/png"

                extras_dict = {'collection_name': collection[0], 'collection_id': collection[1], 'collection_description': collection[2], 'spatial': '{"type":"Polygon", "coordinates":[[[' + lon_min + ', ' + lat_max + '], [' + lon_max + ', ' + lat_max + '], [' + lon_max + ', ' + lat_min + '], [' + lon_min + ', ' + lat_min + '], [' + lon_min + ', ' + lat_max + ']]]}', 'timerange_start': start_time, 'timerange_end': stop_time, 'identifier': identifier, 'filename': filename, 'iso_topic_category': 'oceans', 'institution': 'Nansen Environmental and Remote Sensing Center (NERSC)', 'contributor_name': contributor_name, 'creator_name': creator_name, 'contributor_email': contributor_email, 'source': 'satellite remote sensing', 'satellite': 'Sentinel-1', 'sensor': 'SAR', 'processing_level': 'Level 3', 'groups': [{"name":"cold_regions"}], 'is_output': True}

                package_dict = {}
                package_dict['name'] = extras_dict['identifier'].lower()
                package_dict['title'] = extras_dict['collection_name']
                package_dict['notes'] = extras_dict['collection_description']
                package_dict['tags'] = [{'name': 'earth science'}, {'name': 'ocean'}, {'name': 'sea ice'}, {'name': 'climate'}, {'name': 'atmosphere'}, {'name': 'meteorology'}]
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


if __name__ == "__main__":

    help_msg = """
                    Usage: $ python coldregions.py <destination_ckan_URL> <destination_ckan_apikey> <organization_id> <collection_id>
                    <destination_ckan_URL> e.g.: "http://your-ckan-catalogue"
                    <destination_ckan_apikey> : get the API Key of your CKAN admin user"
                    <organization_id> e.g.: nersc
                    <collection_id> e.g.: S1_ARCTIC_SEAICEEDGE_CLASSIFICATION, S1_ARCTIC_SEAICEEDGE_CLASSIFICATION_INTAROS_2018 or S1_ARCTIC_SEAICEEDGE_CLASSIFICATION_CAATEX_INTAROS_2019
                """

    args = shlex.split(' '.join(sys.argv[1:]))

    if args[0] == '-h':
        print help_msg
	sys.exit()

    try:
        ckan_url = args[0]
        apikey = args[1]
        owner_org = args[2]
        collection_id = args[3]
    except Exception as e:
        print("Exception: " + str(e) + "\n")
        print help_msg
        sys.exit()

    if collection_id == 'S1_ARCTIC_SEAICEEDGE_CLASSIFICATION':
        harvest_url = 'http://thredds.nersc.no/thredds/catalog/nextgeoss/Svalbard_sea_ice_classification_2018/catalog.xml'
        collection_name = 'Sentinel-1 HH/HV based ice/water classification'
        collection_description = 'Sea ice and water classification in the Arctic using Sentinel-1. These datasets have been generated with support from the European Commission in the Horizon 2020 NextGEOSS project (grant agreement No 730329). The algorithms used for Sentinel-1 SAR noise removal and sea ice classification have been developed in the Research Council of Norway project SONARC (Project No 243608) and the Horizon 2020 SPICES project (grant agreement No 640161).'
    elif collection_id == 'S1_ARCTIC_SEAICEEDGE_CLASSIFICATION_INTAROS_2018':
        harvest_url = 'http://thredds.nersc.no/thredds/catalog/nextgeoss/Sea_ice_and_water_classification_in_the_Arctic_for_INTAROS__2018_field_experiment/catalog.xml'
        collection_name = 'Sea ice and water classification in the Arctic for INTAROS 2018 field experiment'
        collection_description = 'Sea ice and water classification in the Arctic, for INTAROS 2018 field experiment, using Sentinel-1 SAR. Extended Wide (EW) swath images at medium resolution (GRDM). Prior to classification, a thermal noise reduction algorithm is applied. A machine learning algorithm is then used to classify sea ice and open water in the noise corrected images. This data is made freely available by NERSC. User must display this citation in any publication or product using data: "These data were produced with support from the Horizon 2020 NextGEOSS project (Grant Agreement No 730329), and made freely available by NERSC (ref. Frode Monsen, Torill Hamre and Mohamed Babiker at NERSC)."'
    elif collection_id == 'S1_ARCTIC_SEAICEEDGE_CLASSIFICATION_CAATEX_INTAROS_2019':
        harvest_url = 'http://thredds.nersc.no/thredds/catalog/nextgeoss/Sea_ice_and_water_classification_in_the_Arctic_for_CAATEX_INTAROS_2019_field_experiment/catalog.xml'
        collection_name = 'Sea ice and water classification in the Arctic for CAATEX/INTAROS 2019 field experiment'
        collection_description = 'Sea ice and water classification in the Arctic, for CAATEX/INTAROS 2019 field experiment, using Sentinel-1 SAR. Extended Wide (EW) swath images at medium resolution (GRDM). Prior to classification, a thermal noise reduction algorithm is applied. A machine learning algorithm is then used to classify sea ice and open water in the noise corrected images. This data is made freely available by NERSC. User must display this citation in any publication or product using data: "These data were produced with support from the Horizon 2020 NextGEOSS project (Grant Agreement No 730329), and made freely available by NERSC (ref. Frode Monsen, Torill Hamre and Mohamed Babiker at NERSC)."'

    collection = [collection_name, collection_id, collection_description]

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
    upload_to_catalogue(session, files, ckan_url, apikey, owner_org, collection)
    _close_session(session)
