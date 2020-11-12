# -*- coding: utf-8 -*-

import json
import requests
import requests_cache
import logging
import xmltodict
from datetime import datetime
from sqlalchemy import desc
import uuid
from os import path
import mimetypes
import stringcase
import re
import csv
import pickle
import shapely.geometry

requests_cache.install_cache()

from ckan.model import Session
from ckan.model import Package
from ckan.plugins.core import implements
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

log = logging.getLogger(__name__)

def clean_snakecase(og_string):
      og_string = re.sub('[^0-9a-zA-Z]+', '_', og_string).lower()
      sc_string = stringcase.snakecase(og_string).strip("_")
      while "__" in sc_string:
         sc_string = sc_string.replace("__", "_")
      return sc_string

class StationInfo(object):
    """
    docstring
    """
    path_lists = {
        'id': ['wmdr:WIGOSMetadataRecord', 'wmdr:facility', 'wmdr:ObservingFacility', 'gml:identifier', '#text'],
        'spatial': ['wmdr:WIGOSMetadataRecord', 'wmdr:facility', 'wmdr:ObservingFacility', 'wmdr:geospatialLocation', 'wmdr:GeospatialLocation', 'wmdr:geoLocation', 'gml:Point', 'gml:pos']
    }
    def __init__(self, record):
        self.id          = get_field(self.path_lists['id'], record.copy())
        self.spatial     = get_field(self.path_lists['spatial'], record.copy())
        self.deployments = self.set_deployment_list(record)

    def set_deployment_list(self, record):
        deployment_path = ['wmdr:WIGOSMetadataRecord', 'wmdr:facility', 'wmdr:ObservingFacility', 'wmdr:observation']
        for path in deployment_path:
            if path in record:
                record = record[path]
            else:
                return []
        deployment_list = get_field(deployment_path, record)
        if not deployment_list:
            deployment_list = []
        return record if type(record) == list else [record]
    
    def get_deployments(self):
        return self.deployments

class DeploymentInfo(object):
   path_lists = {
      'id': ['wmdr:ObservingCapability', 'wmdr:observation', 'om:OM_Observation', 'om:procedure', 'wmdr:Process', 'wmdr:deployment', 'wmdr:Deployment', '@gml:id'],
      'variable': ['wmdr:ObservingCapability', 'wmdr:observation', 'om:OM_Observation', 'om:observedProperty', '@xlink:href'],
      'affiliation': ['wmdr:ObservingCapability', 'wmdr:programAffiliation', '@xlink:href'],
      'application': ['wmdr:ObservingCapability', 'wmdr:observation', 'om:OM_Observation', 'om:procedure', 'wmdr:Process', 'wmdr:deployment', 'wmdr:Deployment', 'wmdr:applicationArea', '@xlink:href'],
      'spatial': ['wmdr:ObservingCapability', 'wmdr:observation', 'om:OM_Observation', 'om:procedure', 'wmdr:Process', 'wmdr:deployment', 'wmdr:Deployment', 'wmdr:deployedEquipment', 'wmdr:Equipment', 'wmdr:geospatialLocation', 'wmdr:GeospatialLocation', 'wmdr:geoLocation', 'gml:Point', 'gml:pos'],
      't0': ['wmdr:ObservingCapability', 'wmdr:observation', 'om:OM_Observation', 'om:procedure', 'wmdr:Process', 'wmdr:deployment', 'wmdr:Deployment', 'wmdr:validPeriod', 'gml:TimePeriod', 'gml:beginPosition'],
      'tf': ['wmdr:ObservingCapability', 'wmdr:observation', 'om:OM_Observation', 'om:procedure', 'wmdr:Process', 'wmdr:deployment', 'wmdr:Deployment', 'wmdr:validPeriod', 'gml:TimePeriod', 'gml:endPosition'],
      'observation': ['wmdr:ObservingCapability', 'wmdr:observation', 'om:OM_Observation', 'om:procedure', 'wmdr:Process', 'wmdr:deployment', 'wmdr:Deployment', 'wmdr:sourceOfObservation', '@xlink:href'],
      'distance': ['om:OM_Observation', 'om:procedure', 'wmdr:Process', 'wmdr:deployment', 'wmdr:Deployment', 'wmdr:heightAboveLocalReferenceSurface'],
   }
   def __init__(self, session, deployment):
      href_attr = ['variable', 'affiliation', 'application', 'observation']
      for attr, path in self.path_lists.items():
         setattr(self, attr, get_field(path, deployment.copy()))
      
      for attribute in href_attr:
         attr_link = getattr(self, attribute, None)
         if attr_link:
            label = get_label(session, attr_link)
            setattr(self, attribute, label)


def get_field(path_list, json_obj, toPrint=False):
   for path in path_list:
      if path in json_obj:
         json_obj = json_obj[path]
      else:
         return None
   return json_obj


def get_label(session, url):
   req = session.get(url)
   content=req.content.decode('utf-8')
   value_reader = csv.reader(content.splitlines(), delimiter=',', quotechar='"')

   rows = list(value_reader)
   headers = rows[0]
   body = rows[1]

   for i, header in enumerate(headers):
      if 'label' in header:
         return body[i]
   return None   

class OSCARHarvester(NextGEOSSHarvester):
    '''
    A harvester for OSCAR products.
    '''
    implements(IHarvester)

    def info(self):
        info =  {   'name': 'oscar',
                    'title': 'OSCAR Harvester',
                    'description': 'A Harvester for OSCAR Products'
        }
        return info

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'oai_pmh_url' not in config_obj:
                raise ValueError('The parameter oai_pmh_url is required')

            if 'metadata_prefix' not in config_obj:
                raise ValueError('The parameter metadata_prefix is required')

            if 'start_date' in config_obj:
                try:
                    datetime.strptime(config_obj['start_date'],
                                      '%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    raise ValueError('start_date format must be 2018-01-01T00:00:00Z')  # noqa: E501

            
            if type(config_obj.get('update_all', False)) != bool:
                raise ValueError('update_all must be true or false')
        except ValueError as e:
            raise e

        return config
    
    def _get_config(self, harvest_job):
        return json.loads(harvest_job.source.config)
    
    def _get_imported_harvest_objects_by_source(self, source_id):   
        return Session.query(HarvestObject).filter(
            HarvestObject.harvest_source_id == source_id,
            HarvestObject.import_finished is not None)

    def _get_last_harvesting_index(self, source_id, parameter):
        """
        Return the token / restart date of the last product harvested 
        or none if no previous harvesting job
        """
        objects = self._get_imported_harvest_objects_by_source(source_id)
        sorted_objects = objects.order_by(desc(HarvestObject.import_finished))
        last_object = sorted_objects.limit(1).first()
        if last_object is not None:
            index = self._get_object_extra(last_object, parameter , None)
            return index
        else:
            return None

    def get_list_identifiers(self, session, url):
        req = session.get(url)
        json_response = xmltodict.parse(req.text)
        list_identifiers = json_response['OAI-PMH']['ListIdentifiers']
        return list_identifiers
    
    def get_record(self, session, url):
        record_path = ['OAI-PMH', 'GetRecord', 'record', 'metadata']

        req = session.get(url)
        json_response = xmltodict.parse(req.text)
        record = get_field(record_path, json_response.copy())
        return record
    
    def get_resumption_token(self, list_identifiers):
        has_token = 'resumptionToken' in list_identifiers and '#text' in list_identifiers['resumptionToken']
        return list_identifiers['resumptionToken']['#text'] if has_token else None

    def get_station_ids(self, raw_list_ids):
        list_ids = []
        highest_date = ''
        for record in raw_list_ids['header']:
            identifier = record['identifier']
            if '@status' in record and 'deleted' in record['@status']:
                log.debug('Station {} has "deleted" status and thus it will not be collected.'.format(identifier))
            else:
                list_ids.append(identifier)
                highest_date = record['datestamp'] if record['datestamp'] > highest_date else highest_date
        return list_ids, highest_date

    # Required by NextGEOSS base harvester
    def gather_stage(self, harvest_job):
        self.log = logging.getLogger(__file__)
        self.log.debug('SCENT Harvester gather_stage for job: %r', harvest_job)

        self.job = harvest_job
        self.source_config = self._get_config(harvest_job)
        base_url = self.source_config.get('oai_pmh_url')
        metadata_prefix = self.source_config.get('metadata_prefix')
        start_date = self.source_config.get('start_date', None)
        self.update_all =  self.source_config.get('update_all', False)

        last_token = self._get_last_harvesting_index(self.job.source_id, 'token')
        restart_date = self._get_last_harvesting_index(self.job.source_id, 'restart_date')

        if last_token:
            query_url = "{}?verb=ListIdentifiers&ListIdentifiers&resumptionToken={}".format(base_url, last_token)
        elif restart_date:
            query_url = "{}?verb=ListIdentifiers&metadataPrefix={}&from={}".format(base_url, metadata_prefix, restart_date)
        elif start_date:
            query_url = "{}?verb=ListIdentifiers&metadataPrefix={}&from={}".format(base_url, metadata_prefix, start_date)
        else:
            query_url = "{}?verb=ListIdentifiers&metadataPrefix={}".format(base_url, metadata_prefix)

        session = requests_cache.CachedSession()
        self.log.debug('Querying: {}.'.format(query_url))
        raw_list_ids = get_list_identifiers(session, query_url)

        token = self.get_resumption_token(raw_list_ids)
        list_ids, largest_datastamp = self.get_station_ids(raw_list_ids)

        while token or not list_ids:
            query_url = "{}?verb=ListIdentifiers&ListIdentifiers&resumptionToken={}".format(base_url, last_token)
            self.log.debug('No active station found, querying: {}.'.format(query_url))
            raw_list_ids = get_list_identifiers(session, query_url)

            token = self.get_resumption_token(raw_list_ids)
            list_ids, largest_datastamp = self.get_station_ids(raw_list_ids)

        restart_date = restart_date if restart_date else ''
        restart_date = largest_datastamp if largest_datastamp > restart_date else restart_date

        ids = []

        for station in list_ids:
            station_query = '{}verb=GetRecord&metadataPrefix={}&identifier={}'.format(base_url, metadata_prefix, station)
            self.log.debug('Querying station: {}.'.format(station))
            record = self.get_record(session, station_query)
            station_info = StationInfo(record)
            deployments = station_info.get_deployments(session, record)

            for deployment in deployments:
                deployment_info = DeploymentInfo(deployment)
                entry_guid = unicode(uuid.uuid4())
                entry_id = '{}_{}'.format(station_info.id, deployment_info.id)
                entry_name = clean_snakecase(entry_id)
                self.log.debug('Gathering %s', entry_name)

                
                content = {}
                content['station'] = pickle.dumps(station_info)
                content['deployment'] = pickle.dumps(deployment_info)

                package_query = Session.query(Package)
                query_filtered = package_query.filter(Package.name == entry_name)
                package = query_filtered.first()

                if package:
                    # Meaning we've previously harvested this,
                    # but we may want to reharvest it now.
                    previous_obj = Session.query(HarvestObject) \
                        .filter(HarvestObject.guid == entry_guid) \
                        .filter(HarvestObject.current == True) \
                        .first()  # noqa: E712
                    if previous_obj:
                        previous_obj.current = False
                        previous_obj.save()

                    if self.update_all:
                        self.log.debug('{} already exists and will be updated.'.format(
                            entry_name))  # noqa: E501
                        status = 'change'

                    else:
                        self.log.debug(
                            '{} will not be updated.'.format(entry_name))  # noqa: E501
                        status = 'unchanged'

                elif not package:
                    # It's a product we haven't harvested before.
                    self.log.debug(
                        '{} has not been harvested before. Creating a new harvest object.'.  # noqa: E501
                        format(entry_name))  # noqa: E501
                    status = 'new'
                obj = HarvestObject(
                    guid=entry_guid,
                    job=self.job,
                    extras=[
                        HOExtra(key='status', value=status),
                        HOExtra(key='token', value=token),
                        HOExtra(key='restart_date', value=restart_date)

                    ])
                obj.content = json.dumps(content)
                obj.package = None if status == 'new' else package
                obj.save()
                ids.append(obj.id)
        return ids
    

    def fetch_stage(self, harvest_object):
        return True

    def build_spatial(self, spatial_info):
        lat, lon, _ = spatial_info.split(" ")
        shapely_point = shapely.geometry.Point(float(lon), float(lat))
        return json.loads(json.dumps(shapely.geometry.mapping(shapely_point)))
    
    # Required by NextGEOSS base harvester
    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        content = json.loads(content)
        station = pickle.loads(content['station'])
        deployment = pickle.loads(content['deployment'])

        item = {}
        item['collection_id'] = "WMO_INTEGRATED_OBSERVING_SYSTEM_SURFACE_BASED"
        item['collection_name'] = "WMO Integrated Observing System (surface-based part)"
        item['collection_description'] = "Metadata describing observations collected under the auspices of the WMO WIGOS covering atmosphere, land and ocean. Metadata are stored in OSCAR/Surface that refers to data hosted at different data centers distributed globally."

        item['identifier'] = '{}_{}'.format(station.id, deployment.id)
        item['title'] = item['identifier']
        item['name']       = clean_snakecase(item['identifier'])

        notes_tmp1 = "Dataset refers to metadata for the observed variable {variable}, associated with the Network(s)/Program(s) \"{affiliation}\"."
        notes_tmp2 = " The observation was  primarily made for {application}."
        notes1         = notes_tmp1.format(variable=deployment.variable, affiliation=deployment.affiliation)
        notes2         = notes_tmp2.format(application=deployment.application) if deployment.application else ""
        item['notes']  = notes1 + notes2
        item['tags']   = []

        item['timerange_start'] = deployment.t0

        if deployment.tf:
            item['timerange_end'] = deployment.tf
        
        if deployment.spatial:
            spatial = self.build_spatial(deployment.spatial)
        else:
            spatial = self.build_spatial(station.spatial)
        item['spatial'] = json.dumps(spatial)

        ####### OPTIONAL FIELDS ########
        item['wigos_id'] = station.id

        if deployment.distance:
            item['distance_from_reference_surface'] = deployment.distance
        if deployment.observation:
            item['source_of_observation'] = deployment.observation
        
        item['resource'] = self.parse_resources(item['wigos_id']) 

    def parse_resources(self, wigos_id):
        resources = []
        resources.append({
                "name": "Website",
                "description": "Station report as html",
                "url": "https://oscar.wmo.int/surface/#/search/station/stationReportDetails/{wigos_id}".format(wigos_id=wigos_id)
        })

        resources.append({
                "name": "WMDR XML",
                "description": "Station report as WMDR XML",
                "url": "https://oscar.wmo.int/oai/provider?verb=GetRecord&metadataPrefix=wmdr&identifier=oai:meteoswiss.ch:{wigos_id}".format(wigos_id=wigos_id)
        })
        return resources

    # Required by NextGEOSS base harvester
    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        return metadata['resource']
            
