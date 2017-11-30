import logging
import hashlib
import os
import json
import requests
import datetime
import uuid
import utils
import uuid as uuid_gen
import logging

import ckan.plugins as plugins
from ckan import plugins as p

from ckanext.nextgeossharvest.lib.esa_base import SentinalHarvester
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



class ESAHarvester(SentinalHarvester, SingletonPlugin):
    '''
    A Harvester for ESA Sentinel Products.
    '''
    implements(IHarvester)

    def info(self):
        return {
            'name': 'esasentinel',
            'title': 'ESA Sentinel Harvester NEw',
            'description': 'A Harvester for ESA Sentinel Products'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)
            print config_obj
            # user and password
            if not 'username' in config_obj:
                raise ValueError('provide username in the harvester configuration')
            if not 'password' in config_obj:
                raise ValueError('provider password in the harvester configuration')

        except ValueError, e:
            raise e

        return config



    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.ESASentinel.gather')
        log.debug('ESASentinelHarvester gather_stage for job: %r', harvest_job)

        self.harvest_job = harvest_job

        # Get source URL
        source_url = harvest_job.source.url

        config = self._set_source_config(harvest_job.source.config)

        ## TODO: try to ping server

        # get current objects out of db
        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id). \
                                    filter(HarvestObject.current == True). \
                                    filter(HarvestObject.harvest_source_id == harvest_job.source.id)

        guid_to_package_id = {}

        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = set(guid_to_package_id.keys())


        # Get contents
        try:
            ids = []

            sentinel = SentinelMetadataExtractor()
            new_id = sentinel.extractMetadataFromAPIForToday(self.config, source_url)

            log.debug('Starting gathering for %s' % source_url)
            guids_in_harvest = set(new_id)

            new = guids_in_harvest - guids_in_db
            delete = guids_in_db - guids_in_harvest
            change = guids_in_db & guids_in_harvest


            for guid in new:
                obj = HarvestObject(guid=guid, job=harvest_job,
                                    extras=[HOExtra(key='status', value='new')])
                obj.save()

                total_results = self.get_total_datasets(config)
                ids.append(self.parse_save_entry_data(config, source_url, obj, total_results))

            for guid in change:
                # obj = HarvestObject(guid=guid, job=harvest_job,
                #                     package_id=guid_to_package_id[guid],
                #                     extras=[HOExtra(key='status', value='change')])
                # obj.save()
                # ids.append(obj.id)
                pass
            for guid in delete:
                # obj = HarvestObject(guid=guid, job=harvest_job,
                #                     package_id=guid_to_package_id[guid],
                #                     extras=[HOExtra(key='status', value='delete')])
                # model.Session.query(HarvestObject). \
                #     filter_by(guid=guid). \
                #     update({'current': False}, False)
                # obj.save()
                # ids.append(obj.id)
                pass

            if len(ids) == 0:
                self._save_gather_error('No records received from SciHub', harvest_job)
                return None
            return ids

        except Exception, e:
            import traceback
            traceback.print_exc()
            log.debug("Some error")
            log.info('MESSAGE' + str(e.args))
            log.info(e.message)
            self._save_gather_error('Unable to get content for URL: %s: %r' % \
                                    (source_url, e), harvest_job)
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


        ###### FINISHED ADD EXTRAS
        # Build the package dict

        print status

        if status == 'new':
            package_schema = logic.schema.default_create_package_schema()
            tag_schema = logic.schema.default_tags_schema()
            tag_schema['name'] = [not_empty, unicode]
            package_schema['tags'] = tag_schema
            extras_schema = logic.schema.default_extras_schema()
            package_schema['extras'] = extras_schema
            context['schema'] = package_schema
            package_dict = {}

            import uuid
            package_dict['id'] = unicode(uuid.uuid4())
            package_schema['id'] = [unicode]

            source_dataset = model.Package.get(harvest_object.source.id)
            if source_dataset.owner_org:
                package_dict['owner_org'] = source_dataset.owner_org

            package_dict['name'] = uuid.uuid4()
            package_dict['title'] = self._get_object_extra(harvest_object, 'title')
            package_dict['extras'] = self._get_all_extras(harvest_object)

            log.info('Package with GUID %s does not exist, let\'s create it' % harvest_object.guid)
            harvest_object.current = True
            harvest_object.package_id = package_dict['id']
            harvest_object.add()

            model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
            model.Session.flush()

            package_id = p.toolkit.get_action('package_create')(context, package_dict)
            log.info('Created new package %s with guid %s', package_id, harvest_object.guid)
        else:
            pass

        model.Session.commit()

        return True


    def fetch_stage(self, harvest_object):
        return True


    def _get_all_extras(self, harvest_object):
        result = []

        for extra in harvest_object.extras:
            if extra.key not in ('id', 'title', 'status'):
                result.append({'key': extra.key, 'value': extra.value})

        return result


    def _get_user_name(self):
        '''
        Returns the name of the user that will perform the harvesting actions
        (deleting, updating and creating datasets)

        By default this will be the internal site admin user. This is the
        recommended setting, but if necessary it can be overridden with the
        `ckanext.spatial.harvest.user_name` config option, eg to support the
        old hardcoded 'harvest' user:

           ckanext.spatial.harvest.user_name = harvest

        '''
        if self._user_name:
            return self._user_name

        context = {'model': model,
                   'ignore_auth': True,
                   'defer_commit': True,  # See ckan/ckan#1714
                   }
        self._site_user = p.toolkit.get_action('get_site_user')(context, {})

        self._user_name = self._site_user['name']

        return self._user_name