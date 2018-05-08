from ckan.plugins.core import SingletonPlugin, implements
import logging
import hashlib
import ckan.plugins as plugins

from ckan import model
from ckan.lib.navl.validators import not_empty

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.spatial.harvesters.base import SpatialHarvester
import uuid as uuid_gen

import utils

from ckan import logic

import os
import json
import requests
import datetime
from datetime import date, timedelta
from bs4 import BeautifulSoup
from ckanext.nextgeossharvest.lib.probav_base import PROBAVBase

#formatter= logging.Formatter('%(levelname)s | %(message)s')
#https://docs.python.org/2/howto/logging.html

#def setup_logger(name, log_file, level=logging.INFO):
#    """Function setup as many loggers as you want"""

#    handler = logging.FileHandler(log_file)        
#    handler.setFormatter(formatter)

#    logger = logging.getLogger(name)
#    logger.setLevel(level)
#    logger.addHandler(handler)

#    return logger

#logger = setup_logger('loggerdataproviders4', '/var/log/harvesters/dataproviders_info.log')


class PROBAVHarvester(PROBAVBase):
    '''
    A Harvester for Proba-V Products.
    '''
    implements(IHarvester)

    def info(self):
        return {
            'name': 'proba-v',
            'title': 'Proba-V Harvester',
            'description': 'A Harvester for Proba-V Products'
            }
        
        
    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'DATE_MIN' in config_obj:
                try:
                    if config_obj['DATE_MIN'] != 'YESTERDAY':
                        datetime.datetime.strptime(config_obj['DATE_MIN'], '%Y-%m-%d')
                except ValueError:
                    raise ValueError('DATE_MIN must be in the format YYYY-MM-DD or the keyword YESTERDAY')

            if 'DATE_MAX' in config_obj:
                try:
                    if config_obj['DATE_MAX'] != 'TODAY':
                        datetime.datetime.strptime(config_obj['DATE_MAX'], '%Y-%m-%d')
                except ValueError:
                    raise ValueError('DATE_MAX must be in the format YYYY-MM-DD or the keyword TODAY')

            if not 'user' in config_obj:
                raise ValueError('user must be set')
            if not 'password' in config_obj:
                raise ValueError('password must be set')

        except ValueError, e:
            raise e

        return config
    
    
    def _make_requests_and_parse_responses_creating_objects(self, url, collection, user, password, products_list):
        # Inital HTTP Request
        timestamp = datetime.datetime.now()
        r = requests.get(url)
        #logger.info('%-12s' % ('vito') + ' | ' + str(timestamp) + ' | ' + str(r.status_code) + ' | ' + str(r.elapsed.total_seconds())+'s')


        # XML Parsing to the first HTTP response.
        soup = BeautifulSoup(r.text, 'lxml-xml')
    
        dataset_num = 0
        while True:
        # Populate the List
            dataset_num = dataset_num + len(soup.find_all('entry'))
            for e in soup.find_all('entry'):
                if 'S1' in collection or 'S5' in collection or 'S10' in collection:
                    timestamp = datetime.datetime.now()
                    r2 = requests.get(e.find(rel='enclosure')['href'],auth=(user, password))
                    #logger.info('%-12s' % ('vito') + ' | ' + str(timestamp) + ' | ' + str(r2.status_code) + ' | ' + str(r2.elapsed.total_seconds())+'s')
                    soup2 = BeautifulSoup(r2.text, 'lxml-xml')
                    for file in soup2.find_all('file'):
                        if '.HDF5' in file['name']:
                            metadata = {}
                            metadata['datasetname'] = file['name']
                            metadata['Collection'] = collection.replace('urn:ogc:def:EOP:VITO:','')
                            metadata['uuid'] = file['name'].lower().replace('.hdf5', '')
                            metadata['StartTime'] = e.date.get_text().split('Z/')[0]
                            metadata['StopTime'] = e.date.get_text().split('Z/')[1][:-1]
                            tile = file['name'].split('_')[3]
                            numX = int(tile[1:3])
                            numY = int(tile[4:])
                            for x in range(36):
                                if numX == x:
                                    for y in range(14):
                                        if numY == y:
                                            lng_min = str(-180.0 + (10*x))
                                            lng_max = str(-170.0 + (10*x))
                                            lat_max = str(75 - (10*y))
                                            lat_min = str(65 - (10*y))
                                            break
                            metadata['Coordinates'] = [[lng_min,lat_max], [lng_max,lat_max], [lng_max,lat_min], [lng_min, lat_min], [lng_min,lat_max]]
                            thumbnail_without_bbox = e.find(rel='icon')['href'].split('BBOX=')[0]
                            thumbnail_url = thumbnail_without_bbox + 'BBOX=' + lat_min + ',' + lng_min + ',' + lat_max + ',' + lng_max + '&HEIGHT=200&WIDTH=200'
                            metadata['thumbnail'] = thumbnail_url
                            metadata['downloadLink'] = file.url.text
                            products_list.append(metadata)
                else:
                    metadata = {}
                    metadata['Collection'] = collection.replace('urn:ogc:def:EOP:VITO:','')
                    metadata['uuid'] = e.id.get_text().split(':')[6] + '_' + e.id.get_text().split(':')[7]
                    metadata['uuid'] = metadata['uuid'].lower()
                    metadata['StartTime'] = e.date.get_text().split('Z/')[0]
                    metadata['StopTime'] = e.date.get_text().split('Z/')[1][:-1]
                    lat_min = e.box.get_text().split(' ')[0]
                    lng_min = e.box.get_text().split(' ')[1]
                    lat_max = e.box.get_text().split(' ')[2]
                    lng_max = e.box.get_text().split(' ')[3]
                    metadata['Coordinates'] = [[lng_min,lat_max], [lng_max,lat_max], [lng_max,lat_min], [lng_min, lat_min], [lng_min,lat_max]]
                    metadata['metadataLink'] = None
                    try:
                        #metadata['metadataLink'] = e.find(title='Inspire')['href']
                        metadata['metadataLink'] = e.find(title='HMA')['href']
                    except Exception:
                        pass
                    #r2 = requests.get(e.find(rel='enclosure')['href'],auth=(user, password))
                    #soup2 = BeautifulSoup(r2.text, 'lxml-xml')
                    metadata['thumbnail'] = None
                    try:
                        height_ql = int(e.find(rel='icon')['href'].split('HEIGHT=')[1].split('&')[0]) * 2
                        width_ql = int(e.find(rel='icon')['href'].split('WIDTH=')[1]) * 2
                        metadata['thumbnail'] = e.find(rel='icon')['href'].split('HEIGHT=')[0] + 'HEIGHT=' + str(height_ql) + '&WIDTH=' + str(width_ql)
                    except Exception:
                        pass
                    #for file in soup2.find_all('file'):
                    #    if '.HDF5' in file['name']:
                    metadata['datasetname'] = e.title.get_text()
                    metadata['downloadLink'] = e.find(rel='enclosure')['href']
                            #metadata['downloadLink'] = file.url.text
                    #    elif '.xml' in file['name'] and 'INSPIRE' not in file['name'] and metadata['metadataLink'] == None:                                
                    #        metadata['metadataLink'] = file.url.text
                    #    elif '.tiff' in file['name'] and metadata['thumbnail'] == None:
                    #        metadata['thumbnail'] = file.url.text
                    products_list.append(metadata)

            # Check if there are more entries than the ones
            # available in the actual response.
            # Stop the loop if the actual response is the last.
            if soup.find(rel='next') is None:
                print dataset_num
                break
            # Make one more request if the actual is not the last.
            timestamp = datetime.datetime.now()
            r = requests.get(soup.find(rel='next')['href'])
            #logger.info('%-12s' % ('vito') + ' | ' + str(timestamp) + ' | ' + str(r.status_code) + ' | ' + str(r.elapsed.total_seconds())+'s')
            soup = BeautifulSoup(r.text, 'lxml-xml')
    
    def gather_stage(self,harvest_job):
        log = logging.getLogger(__name__ + '.gather')
        log.debug('ProbaVHarvester gather_stage for job: %r', harvest_job)

        self.harvest_job = harvest_job

        # Get source URL
        source_url = harvest_job.source.url

        self._set_source_config(harvest_job.source.config)

        # get current objects out of db
        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id).filter(HarvestObject.current==True).\
                                    filter(HarvestObject.harvest_source_id==harvest_job.source.id)

        guid_to_package_id = dict((res[0], res[1]) for res in query)
        current_guids = set(guid_to_package_id.keys())
        current_guids_in_harvest = set()


        # Get contents
        try:
            ids = []
            
            today = datetime.datetime.today().strftime('%Y-%m-%d')
            
            yesterday_d = datetime.datetime.today() - timedelta(1)
            yesterday = yesterday_d.strftime('%Y-%m-%d')
            
            config = self.source_config
    
            if not config:
                pass #leave today and yesterday as is
            else:
                if config.get('DATE_MIN') != 'YESTERDAY':
                    yesterday =  config['DATE_MIN']
                
                if config['DATE_MAX'] != 'TODAY':
                    today =  config['DATE_MAX']
                    
                user = config['user']
                password = config['password']
         
            NOW = str(today)+'T00:00:00.000Z'#Thh:mm:ss.SSSZ
            YESTERDAY = str(yesterday)+'T00:00:00.000Z'#Thh:mm:ss.SSSZ

            products_list = []
            # 
            product_collections = ["urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001", "urn:ogc:def:EOP:VITO:PROBAV_S1-TOC_1KM_V001", "urn:ogc:def:EOP:VITO:PROBAV_S1-TOA_1KM_V001", "urn:ogc:def:EOP:VITO:PROBAV_S10-TOC_1KM_V001", "urn:ogc:def:EOP:VITO:PROBAV_S10-TOC-NDVI_1KM_V001", "urn:ogc:def:EOP:VITO:PROBAV_L2A_1KM_V001", "urn:ogc:def:EOP:VITO:PROBAV_P_V001", "urn:ogc:def:EOP:VITO:PROBAV_S1-TOC_333M_V001", "urn:ogc:def:EOP:VITO:PROBAV_S1-TOA_333M_V001", "urn:ogc:def:EOP:VITO:PROBAV_S10-TOC_333M_V001", "urn:ogc:def:EOP:VITO:PROBAV_S10-TOC-NDVI_333M_V001", "urn:ogc:def:EOP:VITO:PROBAV_S1-TOC_100M_V001", "urn:ogc:def:EOP:VITO:PROBAV_S1-TOA_100M_V001", "urn:ogc:def:EOP:VITO:PROBAV_S1-TOC-NDVI_100M_V001", "urn:ogc:def:EOP:VITO:PROBAV_S5-TOC_100M_V001", "urn:ogc:def:EOP:VITO:PROBAV_S5-TOA_100M_V001", "urn:ogc:def:EOP:VITO:PROBAV_S5-TOC-NDVI_100M_V001", "urn:ogc:def:EOP:VITO:PROBAV_L2A_100M_V001"]

            #os.system('mkdir /tmp/probav')
            for collection in product_collections:
              try:
                url = 'http://www.vito-eodata.be/openSearch/findProducts.atom?collection=' + collection + '&platform=PV01&start=' + YESTERDAY + '&end=' + NOW + '&count=500'

                self._make_requests_and_parse_responses_creating_objects(url, collection, user, password, products_list)

              except Exception as err:
                  #pass
                  print err

            for dataset in products_list:
                hash = hashlib.md5(json.dumps(dataset)).hexdigest() #hash content data
                if hash in current_guids:
                    current_guids_in_harvest.add(hash)
                else:
                    try:
                        obj = HarvestObject(job=harvest_job, guid=hash, extras=[
                            HOExtra(key='status', value='new'),
                            HOExtra(key='uuid', value=dataset['uuid']),
                            HOExtra(key='download_link', value=dataset['downloadLink']),
                            HOExtra(key='metadata_link', value=dataset['metadataLink']),
                            HOExtra(key='dataset_name', value=dataset['datasetname']),
                            HOExtra(key='original_metadata', value=json.dumps(dataset)),
                            HOExtra(key='original_format', value='hdf5')
                        ])
                    except Exception:
                        obj = HarvestObject(job=harvest_job, guid=hash, extras=[
                            HOExtra(key='status', value='new'),
                            HOExtra(key='uuid', value=dataset['uuid']),
                            HOExtra(key='download_link', value=dataset['downloadLink']),
                            HOExtra(key='dataset_name', value=dataset['datasetname']),
                            HOExtra(key='original_metadata', value=json.dumps(dataset)),
                            HOExtra(key='original_format', value='hdf5')
                        ])

                obj.save()
                ids.append(obj.id)

            return ids

        except Exception,e:
            import traceback
            traceback.print_exc()
            log.debug("Some error")
            log.info('MESSAGE' + str(e.args))
            log.info(e.message)
            self._save_gather_error('Unable to get content for URL: %s: %r' % \
                                        (source_url, e),harvest_job)
            return None



    def import_stage(self, harvest_object):
        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
        }

        log = logging.getLogger(__name__ + '.import')
        log.debug('Import stage for harvest object: %s', harvest_object.id)

        if not harvest_object:
            log.error('No harvest object received')
            return False

        self._set_source_config(harvest_object.source.config)


        status = self._get_object_extra(harvest_object, 'status')

        # Get the last harvested object (if any)
        previous_object = model.Session.query(HarvestObject) \
                          .filter(HarvestObject.guid==harvest_object.guid) \
                          .filter(HarvestObject.current==True) \
                          .first()

        if status == 'delete':
            # Delete package
            context.update({
                'ignore_auth': True,
            })
            plugins.toolkit.get_action('package_delete')(context, {'id': harvest_object.package_id})
            log.info('Deleted package {0} with guid {1}'.format(harvest_object.package_id, harvest_object.guid))
 
            return True



        # Check if it is a esa-safe file
        original_document = self._get_object_extra(harvest_object, 'original_document')
        original_format = self._get_object_extra(harvest_object, 'original_format')
        original_metadata = self._get_object_extra(harvest_object, 'original_metadata')


        # Parse document

        metadata = json.loads(original_metadata)
        uuid = self._get_object_extra(harvest_object, 'uuid')


        # Flag previous object as not current anymore

        if previous_object and not self.force_import:
            previous_object.current = False
            previous_object.add()

        # Update GUID with the one on the document
        iso_guid = uuid #iso_values['guid']
        if iso_guid and harvest_object.guid != iso_guid:
            # First make sure there already aren't current objects
            # with the same guid
            existing_object = model.Session.query(HarvestObject.id) \
                            .filter(HarvestObject.guid==iso_guid) \
                            .filter(HarvestObject.current==True) \
                            .first()
            if existing_object:
                self._save_object_error('Object {0} already has this guid {1}'.format(existing_object.id, iso_guid),
                        harvest_object, 'Import')
                return False

            harvest_object.guid = iso_guid
            harvest_object.add()

        # Generate GUID if not present (i.e. it's a manual import)
        if not harvest_object.guid:
            m = hashlib.md5()
            m.update(harvest_object.content.encode('utf8', 'ignore'))
            harvest_object.guid = m.hexdigest()
            harvest_object.add()



        
        now = datetime.datetime.now()
        try:
            metadata_modified_date = now
        except ValueError:
            self._save_object_error('Could not extract reference date for object {0} ({1})'
                        .format(harvest_object.id, now['metadata-date']), harvest_object, 'Import')
            return False

        harvest_object.metadata_modified_date = metadata_modified_date
        harvest_object.add()


        # Build the package dict (deimos part)

        ####ADD EXTRAS

        #########
        #TODO NEED TO ADD LICENSE TYPE on dataset
        ########

        
        coords_checked = self.checkIfCoordsAreCircular(metadata['Coordinates'])
        coords_final = self.createStringCoords(coords_checked)
        
        dataset_name = self._get_object_extra(harvest_object, 'dataset_name')

        spatial_json="{\"type\":\"Polygon\",\"crs\":{\"type\":\"EPSG\",\"properties\":{\"code\":4326,\"coordinate_order\":\"Long,Lat\"}},\"coordinates\":["+str(coords_final)+"]}"


        #log.debug(extras_dict)
        #print extras_dict

        tags_dict = [{"name": "Proba-V"}]

        if 'l2a' in dataset_name.lower():
            tags_dict.append({"name": "L2A"})
        elif 's1_toc' in dataset_name.lower():
            tags_dict.append({"name": "S1-TOC"})
        elif 's1_toa' in dataset_name.lower():
            tags_dict.append({"name": "S1-TOA"})
        elif 's10_toc' in dataset_name.lower():
            tags_dict.append({"name": "S10-TOC"})
        elif 's5_toc' in dataset_name.lower():
            tags_dict.append({"name": "S5-TOC"})
        elif 's5_toa' in dataset_name.lower():
            tags_dict.append({"name": "S5-TOA"})
        elif '_p_' in dataset_name.lower():
            tags_dict.append({"name": "L1C"})

        if '1km' in dataset_name.lower():
            tags_dict.append({"name": "1KM"})
        elif '333m' in dataset_name.lower():
            tags_dict.append({"name": "333M"})
        elif '100m' in dataset_name.lower():
            tags_dict.append({"name": "100M"})

        if 'ndvi' in dataset_name.lower():
            tags_dict.append({"name": "NDVI"})

        try:
            thumbnail= metadata['thumbnail']
            extras_dict = self.generateExtrasDict(name=dataset_name.upper(),metadata=metadata,thumbnail=thumbnail, spatial=spatial_json)
        except Exception:
            extras_dict = self.generateExtrasDict(name=dataset_name.upper(),metadata=metadata,spatial=spatial_json)

        ###### FINISHED ADD EXTRAS
        # Build the package dict
        package_dict = self.get_package_dict(metadata, harvest_object, extras_dict, tags_dict)

        if not package_dict:
            log.error('No package dict returned, aborting import for object {0}'.format(harvest_object.id))
            return False

        # Create / update the package
        context.update({
           'extras_as_string': True,
           'api_version': '2',
           'return_id_only': True})

        if self._site_user and context['user'] == self._site_user['name']:
            context['ignore_auth'] = True


        # The default package schema does not like Upper case tags
        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty, unicode]

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        if status == 'new':
            package_schema = logic.schema.default_create_package_schema()
            package_schema['tags'] = tag_schema
            context['schema'] = package_schema

            # We need to explicitly provide a package ID, otherwise ckanext-spatial
            # won't be be able to link the extent to the package.
            package_dict['id'] = unicode(uuid_gen.uuid4())
            package_schema['id'] = [unicode]

            # Save reference to the package on the object
            harvest_object.package_id = package_dict['id']
            harvest_object.add()
            # Defer constraints and flush so the dataset can be indexed with
            # the harvest object id (on the after_show hook from the harvester
            # plugin)
            model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
            model.Session.flush()

            try:
                package_id = plugins.toolkit.get_action('package_create')(context, package_dict)
                log.info('Created new package %s with guid %s', package_id, harvest_object.guid)

                for key,value in metadata.iteritems():
                    #create resources
                    if key == 'metadataLink':
                        resource_dict={}
                        resource_dict['package_id'] = package_id
                        resource_dict['url'] = value
                        resource_dict['name'] = 'Metadata Download'
                        resource_dict['format'] = 'xml'
                        resource_dict['mimetype'] = 'application/xml'
                        plugins.toolkit.get_action('resource_create')(context,resource_dict)

                    if key == 'downloadLink':
                        resource_dict={}
                        resource_dict['package_id'] = package_id
                        resource_dict['url'] = value
                        resource_dict['name'] = 'Product Download'
                        resource_dict['format'] = 'hdf5'
                        resource_dict['mimetype'] = 'application/x-hdf5'
                        plugins.toolkit.get_action('resource_create')(context,resource_dict)

                    if key == 'thumbnail':
                        resource_dict={}
                        resource_dict['package_id'] = package_id
                        resource_dict['url'] = value
                        resource_dict['name'] = 'Thumbnail Link'
                        resource_dict['format'] = 'png'
                        resource_dict['mimetype'] = 'image/png'
                        plugins.toolkit.get_action('resource_create')(context,resource_dict)

            except Exception as err:
                log.info(err)
                log.info("Error on product " + dataset_name)
                plugins.toolkit.get_action('package_delete')(context, package_dict)

            #except plugins.toolkit.ValidationError, e:
            #    self._save_object_error('Validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
            #    return False

        elif status == 'change':

            # Check if the modified date is more recent
            if not self.force_import and previous_object and harvest_object.metadata_modified_date <= previous_object.metadata_modified_date:

                # Assign the previous job id to the new object to
                # avoid losing history
                harvest_object.harvest_job_id = previous_object.job.id
                harvest_object.add()

                # Delete the previous object to avoid cluttering the object table
                previous_object.delete()

                log.info('Document with GUID %s unchanged, skipping...' % (harvest_object.guid))
            else:
                package_schema = logic.schema.default_update_package_schema()
                package_schema['tags'] = tag_schema
                context['schema'] = package_schema

                package_dict['id'] = harvest_object.package_id
                try:
                    package_id = plugins.toolkit.get_action('package_update')(context, package_dict)
                    log.info('Updated package %s with guid %s', package_id, harvest_object.guid)
                except plugins.toolkit.ValidationError, e:
                    self._save_object_error('Validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                    return False

        model.Session.commit()


        return True
    ##

    def fetch_stage(self, harvest_object):
        return True

