 
# -*- coding: utf-8 -*-
#!/usr/bin/python

"""
---------------------------------------------------------------------
– Project : Service4EO
---------------------------------------------------------------------
– Author : João Andrade
– Issue : 0.1
– Date : 11/09/2020
– Purpose : Publish the outputs of Service4EO Pilot into NextGEOSS Catalogue
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
import xmltodict


def _create_session():
    """Creates a requests session for NextGEOSS catalogue

    Returns:
        A requests session
    """
    s = requests.Session()
    s.verify = False
    return s


def _close_session(session):
    """Closes an active requests MELOA's portal session"""
    session.close()


def _organization_list(session, ckan_url, api_key):
    """List organizations on NextGEOSS CKAN catalogue"""

    headers = {
        'Cache-Control': 'no-store'
    }
    group_rst = session.get(
        ckan_url + '/api/action/organization_list', headers=headers)

    return group_rst.json()['result']


def _create_organization(provider, name_id, session, ckan_url, api_key):
    """Creates an organization on NextGEOSS CKAN catalogue"""

    if name_id in _organization_list(session, ckan_url, api_key):
        print('Org '+name_id+' already exists. Skipping...')
        return name_id

    params = {
        'name': name_id,
        'title': provider
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


def _create_dataset(provider, package_dict, session, ckan_url, api_key):
    """Creates a dataset on NextGEOSS CKAN catalogue"""

    org_id = _create_organization(provider, package_dict['owner_org'], session, ckan_url, api_key)
    if org_id == "":
        print('Could not create datasets in the catalogue due to organization issues.\n')
        sys.exit()

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


def _get_attr(query, dict_xml):
    """
    Fetch the query field, by tag
    :param query: tag to look for at first level
    :param dict_xml: xml dict
    :param tag_xml: tag of the second level to returnname_id
    :return: the appropriate element
    """
    res = list(gen_dict_extract(query, dict_xml))
    if not res:
        return None
    else:
        return res[0]


def gen_dict_extract(key, var):
    """
    Method to fastly iterate over a dictionary to find a specific key
    from: https://stackoverflow.com/a/29652561
    The dict can contain both other dicts, as lists
    :param key: value to search inside the dict
    :param var: dict to search
    :return:
    """
    if hasattr(var, 'items'):
        for k, v in var.items():
            if k == key:
                yield v
            if isinstance(v, dict):
                for result in gen_dict_extract(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in gen_dict_extract(key, d):
                        yield result


def get_file_metadata(dict_pilot):
    """Get metadata from the input XML file"""

    title = _get_attr('File_Name', dict_pilot)
    identifier = title
    name = identifier
    
    notes = _get_attr('File_Description', dict_pilot)
    collection_description = notes
    collection_name = collection_description
    coordinates = _get_attr('gml:posList', dict_pilot)
    coordinates = coordinates.split(' ')
    coordinates_num = len(coordinates)
    x = 0
    spatial = '{"type":"Polygon", "coordinates":[[['
    while x < coordinates_num:
        spatial = spatial + coordinates[x] + ', ' + coordinates[x+1]
        x += 2
        if x != coordinates_num:
            spatial = spatial + '], [' 
    spatial = spatial + ']]]}'

    timerange_start = _get_attr('Validity_Start', dict_pilot)
    timerange_end = _get_attr('Validity_Stop', dict_pilot)

    mission = _get_attr('Mission', dict_pilot)
    file_class = _get_attr('File_Class', dict_pilot)
    file_type = _get_attr('File_Type', dict_pilot)
    file_version = _get_attr('File_Version', dict_pilot)
    system = _get_attr('System', dict_pilot)
    owner_org = system.lower()
    creator = _get_attr('Creator', dict_pilot)
    creation_date = _get_attr('Creation_Date', dict_pilot)
    doi = _get_attr('eop:doi', dict_pilot)
    cloud_coverage = _get_attr('Notes', dict_pilot)
    parent_identifier = _get_attr('eop:parentIdentifier', dict_pilot)
    collection_id = parent_identifier
    acquisition_type = _get_attr('eop:acquisitionType', dict_pilot)
    product_type = _get_attr('eop:productType', dict_pilot)
    status = _get_attr('eop:status', dict_pilot)
    relative_orbit_number = _get_attr('eop:statusDetail', dict_pilot)
    acquisition_station = _get_attr('eop:acquisitionStation', dict_pilot)
    acquisition_date = _get_attr('eop:acquisitionDate', dict_pilot)
    archiving_date = _get_attr('eop:archivingDate', dict_pilot)
    image_quality_degradation = _get_attr('eop:imageQualityDegradation', dict_pilot)['#text']
    platform_short_name = _get_attr('eop:Platform', dict_pilot)['eop:shortName']
    platform_serial_identifier = _get_attr('eop:serialIdentifier', dict_pilot)
    instrument_short_name = _get_attr('eop:Instrument', dict_pilot)['eop:shortName']
    sensor_type = _get_attr('eop:sensorType', dict_pilot)
    orbit_number = _get_attr('eop:orbitNumber', dict_pilot)
    last_orbit_number = _get_attr('eop:lastOrbitNumber', dict_pilot)
    orbit_direction = _get_attr('eop:orbitDirection', dict_pilot)

    product_url = 'http://31.171.240.90:8080/v1/AUTH_archive/' + _get_attr('eop:ProductInformation', dict_pilot)['eop:fileName']

    extras_dict = {'collection_name': collection_name, 'collection_id': collection_id, 'collection_description': collection_description, 'spatial': spatial, 'timerange_start': timerange_start, 'timerange_end': timerange_end, 'identifier': identifier, 'mission': mission, 'file_class': file_class, 'file_type': file_type, 'file_version': file_version, 'system': system, 'creator': creator, 'creation_date': creation_date, 'doi': doi, 'cloud_coverage': cloud_coverage, 'parent_identifier': parent_identifier, 'acquisition_type': acquisition_type, 'product_type': product_type, 'status': status, 'relative_orbit_number': relative_orbit_number, 'acquisition_station': acquisition_station, 'acquisition_date': acquisition_date, 'archiving_date': archiving_date, 'image_quality_degradation': image_quality_degradation, 'platform_short_name': platform_short_name, 'platform_serial_identifier': platform_serial_identifier, 'instrument_short_name': instrument_short_name, 'sensor_type': sensor_type, 'orbit_number': orbit_number, 'last_orbit_number': last_orbit_number, 'orbit_direction': orbit_direction, 'is_output': True}

    package_dict = {}
    package_dict['name'] = extras_dict['identifier'].lower()
    package_dict['title'] = title
    package_dict['notes'] = notes
    package_dict['tags'] = [{'name': mission}, {'name': product_type}, {'name': platform_short_name}, {'name': instrument_short_name}, {'name': sensor_type}]
    if 'groups' in extras_dict:
        package_dict['groups'] = extras_dict['groups']
    package_dict['extras'] = _get_extras(extras_dict)
    package_dict['resources'] = _set_resources(product_url)
    package_dict['private'] = False
    package_dict['id'] = unicode(uuid.uuid4())  # noqa: F821
    package_dict['owner_org'] = owner_org

    return [system, package_dict]


def _get_extras(extras_dict):
    """Return a list of CKAN extras."""
    skip = {'id', 'title', 'tags', 'status', 'notes', 'name', 'resource', 'groups'}  # noqa: E501
    extras_tmp = [{'key': key, 'value': value} for key, value in extras_dict.items() if key not in skip]
    extras = [{'key': 'dataset_extra', 'value': str(extras_tmp)}]

    return extras


def _set_resources(product_url):
    return [{
            'name': 'Product Download',
            'description': 'Download the tiff file',
            'url': product_url,
            'format': 'TIFF',
            'mimetype': 'image/tiff'
        }]


if __name__ == "__main__":

    help_msg = """
                    Usage: $ python coldregions.py <input_xml_file> <destination_ckan_URL> <destination_ckan_apikey>
                    <input_xml_file> e.g.: "/path/to/sample_xml_file.xml"
                    <destination_ckan_URL> e.g.: "http://your-ckan-catalogue"
                    <destination_ckan_apikey> : get the API Key of your CKAN admin user"
                """

    args = shlex.split(' '.join(sys.argv[1:]))

    if args[0] == '-h':
        print help_msg
        sys.exit()

    try:
        xml_file = args[0]
        ckan_url = args[1]
        api_key = args[2]
    except Exception as e:
        print("Exception: " + str(e) + "\n")
        print help_msg
        sys.exit(1)

    f = open(xml_file, "r")
    xml_string = None
    try:
        xml_string = f.read()
    except Exception as e:
        print("Error: Could not read xml\n")
        print("Exception: " + str(e) + "\n")
        sys.exit(1)

    f.close()

    dict_pilot = None
    try:
        dict_pilot = xmltodict.parse(xml_string)
    except Exception as e:
        print("Error: Could not convert to dict\n")
        print("Exception: " + str(e) + "\n")
        sys.exit(1)

    # transform the ordered dict into a regular dict to get the iterable function to work properly
    dict_pilot = json.loads(json.dumps(dict_pilot))

    metadata = get_file_metadata(dict_pilot)
    provider = metadata[0]
    package_dict = metadata[1]

    session = _create_session()
    _create_dataset(provider, package_dict, session, ckan_url, api_key)
    _close_session(session)
