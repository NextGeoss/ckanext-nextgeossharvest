import logging

import logging
import hashlib
import os
import json
import requests
import datetime
import uuid
import ast
import utils
import uuid as uuid_gen
import logging

import ckan.plugins as plugins
from ckan import plugins as p

from ckanext.nextgeossharvest.lib.vito_base import VitoProbaVHarvester
from ckan import model
from ckan.lib.navl.validators import not_empty
from ckan import logic
from ckan.logic import ValidationError, NotFound, get_action

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.spatial.harvesters.base import SpatialHarvester
from ckan.plugins.core import SingletonPlugin, implements

from requests.auth import HTTPBasicAuth
from pylons import config


log = logging.getLogger(__name__)

class VitoHarvester(VitoProbaVHarvester, SingletonPlugin):

    implements(IHarvester)

    def info(self):
        '''
        Harvesting implementations must provide this method, which will return
        a dictionary containing different descriptors of the harvester. The
        returned dictionary should contain:

        * name: machine-readable name. This will be the value stored in the
          database, and the one used by ckanext-harvest to call the appropiate
          harvester.
        * title: human-readable name. This will appear in the form's select box
          in the WUI.
        * description: a small description of what the harvester does. This
          will appear on the form as a guidance to the user.

        A complete example may be::

            {
                'name': 'csw',
                'title': 'CSW Server',
                'description': 'A server that implements OGC's Catalog Service
                                for the Web (CSW) standard'
            }

        :returns: A dictionary with the harvester descriptors
        '''

        return {
            'name': 'VITO',
            'title': 'Harvester for VITO imagiery',
            'description': 'Harvesting data from VITO'
        }

    def validate_config(self, config):
        '''

        [optional]

        Harvesters can provide this method to validate the configuration
        entered in the form. It should return a single string, which will be
        stored in the database.  Exceptions raised will be shown in the form's
        error messages.

        :param harvest_object_id: Config string coming from the form
        :returns: A string with the validated configuration options
        '''
        pass

    def get_original_url(self, harvest_object_id):
        '''

        [optional]

        This optional but very recommended method allows harvesters to return
        the URL to the original remote document, given a Harvest Object id.
        Note that getting the harvest object you have access to its guid as
        well as the object source, which has the URL.
        This URL will be used on error reports to help publishers link to the
        original document that has the errors. If this method is not provided
        or no URL is returned, only a link to the local copy of the remote
        document will be shown.
        document will be shown.

        Examples:
            * For a CKAN record: http://{ckan-instance}/api/rest/{guid}
            * For a WAF record: http://{waf-root}/{file-name}
            * For a CSW record: http://{csw-server}/?Request=GetElementById&Id={guid}&...

        :param harvest_object_id: HarvestObject id
        :returns: A string with the URL to the original document
        '''
        pass


    def gather_stage(self, harvest_job):
        '''
        The gather stage will receive a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
              stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database, specifying
              the guid and a reference to its job. The HarvestObjects need a
              reference date with the last modified date for the resource, this
              may need to be set in a different stage depending on the type of
              source.
            - creating and storing any suitable HarvestGatherErrors that may
              occur.
            - returning a list with all the ids of the created HarvestObjects.
            - to abort the harvest, create a HarvestGatherError and raise an
              exception. Any created HarvestObjects will be deleted.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        '''

        log = logging.getLogger(__name__ + 'VITO.gather')
        log.debug('VITOHarvester gater_stage for job: %r', harvest_job)

        self.harvest_job = harvest_job

        # Get source url
        source_url = harvest_job.source.url

        self._set_source_config(harvest_job.source.config)

        # Get urls from harvest config
        source_config_obj = json.loads(harvest_job.source.config)
        collection_url =  source_config_obj.get('collection_url', 'findCollections')
        products_url =  source_config_obj.get('products_url', 'findProducts')
        platform =  source_config_obj.get('platform', '')
        platform_name = source_config_obj.get('platform_name', '')

        product_collections = self._get_collections(source_url, collection_url, platform)

        # get list of collections for the platform
        log.info(product_collections)

        #get max_number of datasets to collect per collection
        max_datasets = source_config_obj.get('max_no_datasets', 1)

        #TODO get platform from config

        # get current objects out of db
        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id). \
                                    filter(HarvestObject.current == True). \
                                    filter(HarvestObject.harvest_source_id == harvest_job.source.id)

        guid_to_package_id = {}

        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = set(guid_to_package_id.keys())
        log.info("GUIDS IN DB %s", guids_in_db)

        log.debug('Starting gathering for %s' % source_url)

        ids = []

        # gather datasets for each collection in product_collections
        for collection in product_collections:
            source_url_tmp = source_url + products_url +'.atom?collection=' + collection['id'] + '&platform=' + \
                             platform +'&count=1'
            log.info(source_url_tmp)

            total_datasets = self._get_total_datasets(source_url_tmp)
            log.info('Found %s results', total_datasets)
            total_datasets = 100

            '''
                if the total number of datasets is greater then max_datasets,
                collect max_datasets from the collection.
                set the count depending on the total_datasets:
                - if total_datasets > max_datasets => set count to 500,
                and go to the last page
                - else set count to total_datasets
            '''
            if total_datasets > max_datasets:
                url_tmp = source_url + products_url +'.atom?collection=' + collection['id'] + '&platform=PV01&count=' + str(max_datasets)
                datasets_for_collection = 0

                #get link to last page
                last_page = self._get_last_page(url_tmp)

                request = requests.get(last_page)
                if request.status_code != 200:
                    print
                    'Wrong authentication? status code != 200 - status ' + str(request.status_code)
                    return None

                # get items per page
                items_per_page = self._get_items_per_page(request)
                datasets_for_collection += int(items_per_page)

                # Get contents
                new_id = self._get_harvest_ids(request)
                guids_in_harvest = set(new_id)
                log.info(guids_in_harvest)

                new = guids_in_harvest - guids_in_db
                delete = guids_in_db - guids_in_harvest
                change = guids_in_db & guids_in_harvest
                log.info(change)

                if datasets_for_collection < max_datasets:
                    # get previous page
                    prev = self._get_previous_page(request)

                    request = requests.get(prev)
                    if request.status_code != 200:
                        print
                        'Wrong authentication? status code != 200 - status ' + str(r.status_code)
                        return None

                    items_per_page = self._get_items_per_page(request)
                    datasets_for_collection += int(items_per_page)
                    log.info('IN PREV PAGE')

                    new_id_prev = self._get_harvest_ids(request)
                    guids_in_harvest_prev = set(new_id)
                    log.info(guids_in_harvest_prev)

                    new_prev = guids_in_harvest_prev - guids_in_db
                    new.update(new_prev)

                for guid in new:
                    # create harvest object
                    obj = HarvestObject(guid=guid, job=harvest_job,
                                        extras=[HOExtra(key='status', value='new')])
                    obj.save()

                    item = self._get_entries_from_request(request)
                    title = platform_name + ' ' + collection['title']
                    item['title'] = title.rstrip()
                    print item['productType']

                    description = self._get_dataset_description(item['productType'])
                    notes = str(description) + " " + str(collection['summary']) + " " + str(item['summary'])
                    print notes

                    obj.extras.append(HOExtra(key='notes', value=str(notes)))

                    tags = self._get_tags_for_dataset(item)
                    print tags
                    obj.extras.append(HOExtra(key='tags', value=str(tags)))

                    for k, v in item.items():
                        obj.extras.append(HOExtra(key=k, value=v))

                    print obj
                    ids.append(obj.id)

                log.info("Collected %s datasets for collection %s", str(datasets_for_collection), str(collection))
        return ids


    def fetch_stage(self, harvest_object):
        return True


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

        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty, unicode]
        extras_schema = logic.schema.default_extras_schema()

        if status == 'new':
            package_schema = logic.schema.default_create_package_schema()

            package_schema['tags'] = tag_schema
            package_schema['extras'] = extras_schema

            context['schema'] = package_schema
            package_dict = {}

            # We need to explicitly provide a package ID, otherwise ckanext-spatial
            # won't be be able to link the extent to the package.
            package_dict['id'] = unicode(uuid.uuid4())
            package_schema['id'] = [unicode]

            source_dataset = model.Package.get(harvest_object.source.id)
            if source_dataset.owner_org:
                package_dict['owner_org'] = source_dataset.owner_org

            package_dict['extras'] = self._get_all_extras(harvest_object)
            package_dict['name'] = uuid.uuid4()
            package_dict['title'] = self._get_object_extra(harvest_object, 'title')
            package_dict['notes'] = self._get_object_extra(harvest_object, 'notes')
            package_dict['tags'] = self._get_tags_extra(harvest_object)

            # resource
            resource_url = self._get_object_extra(harvest_object, 'resource')
            resources = [{
                 'name': 'Product Download from Vito',
                 'description': 'Download the product from Vito. NOTE: DOWNLOAD REQUIRES LOGIN',
                 'url': resource_url
            }]
            package_dict['resources'] = resources

            log.info('Package with GUID %s does not exist, let\'s create it' % harvest_object.guid)
            harvest_object.current = True
            harvest_object.package_id = package_dict['id']
            # Defer constraints and flush so the dataset can be indexed with
            # the harvest object id (on the after_show hook from the harvester
            # plugin)
            harvest_object.add()

            model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
            model.Session.flush()
            print package_dict
            package_id = p.toolkit.get_action('package_create')(context, package_dict)
            log.info('Created new package %s with guid %s', package_id, harvest_object.guid)

        model.Session.commit()

        return True


    def _get_all_extras(self, harvest_object):
        result = []

        for extra in harvest_object.extras:
            if extra.key not in ('id', 'title', 'tags', 'status', 'notes', 'resource', 'Platform',
                                 'Polygon', 'Sensor', 'Instrument', 'surfaceMembers', 'Point', 'type'):
                result.append({'key': extra.key, 'value': extra.value})
        return result


    def _get_tags_extra(self, harvest_object):
        tags = []

        for extra in harvest_object.extras:
            if extra.key == 'tags':
                tags = ast.literal_eval(extra.value)
        print tags
        return  tags