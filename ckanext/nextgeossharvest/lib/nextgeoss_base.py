# -*- coding: utf-8 -*-

import ast
import json
import logging
import os
import uuid
from datetime import datetime
from string import Template

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectTimeout, ReadTimeout
from sqlalchemy.sql import bindparam, update

import requests_ftp
import shapely.wkt
from ckan import logic, model
from ckan import plugins as p
from ckan.common import config
from ckan.lib.navl.validators import not_empty
from ckan.model import Package, Session
from ckanext.harvest.harvesters.base import HarvesterBase
from shapely.errors import ReadingError, WKTReadingError

log = logging.getLogger(__name__)


class NextGEOSSHarvester(HarvesterBase):
    """
    Base class for all NextGEOSS harvesters including helper methods and
    methods that all harvesters must call. We may want to move some of
    SentinelHarvester's methods (see esa_base.py) to this class.
    """

    def _get_object_extra(self, harvest_object, key, default=None):
        """
        Helper method for retrieving the value from a harvest object extra.
        """
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return default

    def _get_package_extra(self, package_dict, flagged_extra, default=None):
        """
        Helper method for retrieving the value from a package's extras list.
        """
        extras = self.convert_string_extras(package_dict['extras'])

        if "dataset_extra" in extras:
            extras = ast.literal_eval(extras["dataset_extra"])

        if type(extras) == list:
            for extra in extras:
                if extra["key"] == flagged_extra:
                    return extra["value"]
        elif type(extras) == dict:
            value = extras.get(flagged_extra)
            if value:
                return value

        return default

    def convert_string_extras(self, extras_list):
        """Convert extras stored as a string back into a normal extras list."""
        try:
            extras = ast.literal_eval(extras_list[0]["value"])
            assert type(extras) == list
            return extras
        except (Exception, AssertionError):
            return extras_list

    def _set_source_config(self, config_str):
        '''
        Loads the source configuration JSON object into a dict for
        convenient access (borrowed from SpatialHarvester)
        '''
        if config_str:
            self.source_config = json.loads(config_str)
            log.debug('Using config: %r', self.source_config)
        else:
            self.source_config = {}

    def _get_package_dict(self, package):
        """
        Return the full package dict for a given package _object_.

        (We can probably establish the context once for the whole harvester
        and save it as a class attribute, as it seems like it will always be
        the same.)
        """
        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
            'ignore_auth': True
        }

        return logic.get_action('package_show')(context, {'id': package.name})

    def _refresh_harvest_objects(self, harvest_object, package_id):
        """
        Perform harvester housekeeping:
            - Flag the other objects of the source as not current
            - Set a refernce to the package in the harvest object
            - Flag it as current
            - And save the changes
        """
        # Flag the other objects of this source as not current
        from ckanext.harvest.model import harvest_object_table
        u = update(harvest_object_table) \
            .where(harvest_object_table.c.package_id == bindparam('pkg_id')) \
            .values(current=False)
        Session.execute(u, params={'pkg_id': package_id})
        Session.commit()
        # Refresh current object from session, otherwise the
        # import paster command fails
        # (Copied from the Gemini harvester--not sure if necessary)
        Session.remove()
        Session.add(harvest_object)
        Session.refresh(harvest_object)
        # Set reference to package in the HarvestObject and flag it as
        # the current one
        if not harvest_object.package_id:
            harvest_object.package_id = package_id
        harvest_object.current = True
        harvest_object.save()

    def _create_package_dict(self, parsed_content):
        """
        Create a package dictionary using the parsed content.
        The id and owner org will be added later as they are not derived from
        the content.
        """
        package_dict = {}
        package_dict['name'] = parsed_content['name']
        package_dict['title'] = parsed_content['title']
        package_dict['notes'] = parsed_content['notes']
        package_dict['tags'] = parsed_content['tags']
        if 'groups' in parsed_content:
            package_dict['groups'] = parsed_content['groups']
        package_dict['extras'] = self._get_extras(parsed_content)
        package_dict['resources'] = self._get_resources(parsed_content)
        package_dict['private'] = self.source_config.get('make_private', False)
        return package_dict

    def _create_or_update_dataset(self, harvest_object, status):
        """
        Create a data dictionary and then create or update a dataset.
        """
        parsed_content = self._parse_content(harvest_object.content)
        package_dict = self._create_package_dict(parsed_content)

        # Add the harvester ID to the extras so that CKAN can find the
        # harvested datasets in searches for stats, etc.

        # We need to explicitly provide a package ID, otherwise
        # ckanext-spatial won't be be able to link the extent
        # to the package.

        # When updating, never change the iTag tags. Never change the iTag
        # extras. Do not change resources from other harvesters unless forced.
        if status == 'change':
            log.debug('Updating {}'.format(package_dict['name']))
            old_dataset = harvest_object.package
            old_pkg_dict = self._get_package_dict(old_dataset)
            package_dict['id'] = old_dataset.id
            package_dict['owner_org'] = old_dataset.owner_org
            package_dict['tags'] = self._update_tags(old_pkg_dict.get('tags', []),  # noqa: E501
                                                     package_dict['tags'])
            package_dict['extras'] = self._update_extras(old_pkg_dict.get('extras', []),  # noqa: E501
                                                         package_dict['extras'])  # noqa: E501
            package_schema = logic.schema.default_update_package_schema()
            action = 'package_update'
        elif status == 'new':
            log.debug('Creating new dataset for {}'
                      .format(package_dict['name']))
            # Tags, extras, and resources are all new, so we add whatever we
            # get from the parsed content.
            package_dict['id'] = unicode(uuid.uuid4())  # noqa: F821
            package_dict['owner_org'] = model.Package.get(harvest_object.source.id).owner_org  # noqa: E501
            package_schema = logic.schema.default_create_package_schema()
            package_schema['id'] = [unicode]  # noqa: F821
            action = 'package_create'

        # Create context after establishing if we're updating or creating
        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
        }

        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty, unicode]  # noqa: F821
        extras_schema = logic.schema.default_extras_schema()
        package_schema['tags'] = tag_schema
        package_schema['extras'] = extras_schema
        context['schema'] = package_schema

        try:
            package = p.toolkit.get_action(action)(context, package_dict)
        # IMPROVE: I think ckan.logic.ValidationError is the only Exception we
        # really need to worry about. #########################################
        except Exception as e:
            print e
            # Name/URL already in use may just mean that another harvester
            # created the dataset in the meantime. Retry with status 'change'.
            if status == 'new':
                # Check if there's an existing package now
                old_package = Session.query(Package) \
                    .filter(Package.name == package_dict['name']).first()
                if old_package:
                    harvest_object.package = old_package
                    harvest_object.save()
                    return self._create_or_update_dataset(harvest_object,
                                                          'change')
                else:
                    self._save_object_error('Creation error for {}: {}'
                                            .format(package_dict['name'], e.message),  # noqa: E501
                                            harvest_object, 'Import')
                    return None
            else:
                self._save_object_error('Error updating {}: {}'
                                        .format(package_dict['name'], e.message),  # noqa: E501
                                        harvest_object, 'Import')
                return None

        return package

    def _convert_to_geojson(self, spatial):
        """
        Return a GeoJSON polygon if the spatial coordinates are valid.

        Return None if not.
        """
        try:
            coords = shapely.wkt.loads(spatial)
        except (ReadingError, WKTReadingError):
            return None

        coords_type = coords.type.upper()
        if coords_type == 'MULTIPOLYGON':
            c = coords.geoms[0].exterior.coords
            c_list = list(c[0])
        elif coords_type == 'POLYGON':
            c = coords.exterior.coords
            c_list = list(c[0])
        else:
            return None

        # Remove double coordinates -- they are not valid GeoJSON and Solr
        # will reject them.
        coords_list = [c_list]
        for i in c[1:]:
            new_coord = list(i)
            if new_coord != coords_list[-1]:
                coords_list.append(new_coord)

        template = Template('''{"type": "Polygon", "coordinates": [$coords_list]}''')  # noqa: E501
        geojson = template.substitute(coords_list=coords_list)

        return geojson

    def _get_extras(self, parsed_content):
        """Return a list of CKAN extras."""
        skip = {'id', 'title', 'tags', 'status', 'notes', 'name', 'resource', 'groups'}  # noqa: E501
        extras_tmp = [{'key': key, 'value': value}
                      for key, value in parsed_content.items()
                      if key not in skip]
        extras = [{'key': 'dataset_extra', 'value': str(extras_tmp)}]

        return extras

    def _update_tags(self, old_tags, new_tags):
        """
        Add any new tags from the harvester, but preserve existing tags
        so that we don't lose tags from iTag or from other harvesters.
        """
        old_tag_names = {tag['name'] for tag in old_tags}
        for tag in new_tags:
            if tag['name'] not in old_tag_names:
                old_tags.append(tag)
        return old_tags

    def _update_extras(self, old_extras, new_extras):
        """
        Replace the old extras with the new extras from the harvester,
        or extend the new extras with the different fields from
        old_extras
        """

        ignore_list = [
            "StartTime",
            "StopTime",
            "thumbnail",
            "summary",
            "Filename",
            "size",
            "scihub_download_url",
            "scihub_product_url",
            "scihub_manifest_url",
            "scihub_thumbnail",
            "noa_download_url",
            "noa_product_url",
            "noa_manifest_url",
            "noa_thumbnail",
            "code_download_url",
            "code_product_url",
            "code_manifest_url",
            "code_thumbnail",
        ]
        # Robustness for harvesters that do not save configuration
        # into self.source_config
        extend_extras = False
        if hasattr(self, 'source_config'):
            extend_extras = self.source_config.get('multiple_sources', False)

        if extend_extras:
            # For datasets with multiple sources, the extras are expanded
            # with new fields
            if "dataset_extra" in new_extras[0]['key']:
                new_values = eval(new_extras[0]['value'])
            else:
                new_values = new_extras
            new_extra_keys = [new_value['key'] for new_value in new_values]

            for old_extra in old_extras:
                if ((old_extra['key'] not in new_extra_keys) and 
                    (old_extra['key'] not in ignore_list)):
                    new_values.append(old_extra)
            return [{'key': 'dataset_extra', 'value': str(new_values)}]
        else:
            # For datasets with single source, the extras are replaced
            # with the fields collected in the most recent harvest
            return new_extras

    def make_provider_logger(self, filename='dataproviders_info.log'):
        """Create a logger just for provider uptimes."""
        log_dir = config.get('ckanext.nextgeossharvest.provider_log_dir')
        if log_dir:
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            handler = logging.FileHandler('{}/{}'.format(log_dir, filename))
            handler.setFormatter(logging.Formatter('%(levelname)s | %(message)s'))  # noqa: E501
            logger = logging.getLogger('provider_logger')
            logger.setLevel(logging.INFO)
            logger.addHandler(handler)
        else:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(levelname)s | %(message)s'))  # noqa: E501
            logger = logging.getLogger('provider_logger')
            logger.setLevel(logging.INFO)
            logger.addHandler(handler)

        return logger

    # New function added to generate logs about the number of datasets
    # harvested by job for each data source
    def make_harvester_logger(self, filename='dataconnectors_info.log'):
        """Create a logger just for datasets harvested."""
        log_dir = config.get('ckanext.nextgeossharvest.provider_log_dir')
        if log_dir:
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            handler = logging.FileHandler('{}/{}'.format(log_dir, filename))
            handler.setFormatter(logging.Formatter('%(levelname)s | %(message)s'))  # noqa: E501
            logger = logging.getLogger('harvester_logger')
            logger.setLevel(logging.INFO)
            logger.addHandler(handler)
        else:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(levelname)s | %(message)s'))  # noqa: E501
            logger = logging.getLogger('harvester_logger')
            logger.setLevel(logging.INFO)
            logger.addHandler(handler)

        return logger

    def _crawl_urls_ftp(self, url, provider):
        """
        Check if a file is present on an FTP server and return the appropriate
        status code.
        """
        # We need to be able to mock this for testing and requests-mock doesn't
        # work with requests-ftp, so this is our workaround. We'll just bypass
        # this method like so (the real method returns either an int or None):
        test_ftp_status = self.source_config.get('test_ftp_status')
        if test_ftp_status == 'ok':
            return 10000
        elif test_ftp_status == 'error':
            return None

        # And now here's the real method:
        timeout = self.source_config['timeout']
        username = self.source_config['username']
        password = self.source_config['password']

        # Make a request to the website
        timestamp = str(datetime.utcnow())
        log_message = '{:<12} | {} | {} | {}s'
        try:
            requests_ftp.monkeypatch_session()
            s = requests.Session()
            r = s.size(url, auth=HTTPBasicAuth(username, password),
                       timeout=timeout)
            status_code = r.status_code
            elapsed = r.elapsed.total_seconds()
        except (ConnectTimeout, ReadTimeout) as e:
            self._save_gather_error('Request timed out: {}'.format(e), self.job)  # noqa: E501
            status_code = 408
            elapsed = 9999

        if status_code == 213:
            size = int(r.text)
        else:
            size = None

        if status_code not in {213, 408}:
            self._save_gather_error(
                '{} error: {}'.format(status_code, r.text), self.job)

        self.provider_logger.info(log_message.format(provider, timestamp,
                                                     status_code, elapsed))
        return size

    def import_stage(self, harvest_object):
        log = logging.getLogger(__name__ + '.import')
        log.debug('Import stage for harvest object with GUID {}'
                  .format(harvest_object.id))

        # Save a reference (review the utility of this)
        self.obj = harvest_object

        # Provide easy access to the config
        self._set_source_config(harvest_object.source.config)

        if harvest_object.content is None:
            self._save_object_error('Empty content for object {}'
                                    .format(harvest_object.id),
                                    harvest_object, 'Import')
            return False

        status = self._get_object_extra(harvest_object, 'status')

        # Check if we need to update the dataset
        if status != 'unchanged':
            # This can be a hook
            package = self._create_or_update_dataset(harvest_object, status)
            # This can be a hook
            if not package:
                return False
            package_id = package['id']
        else:
            package_id = harvest_object.package.id

        # Perform the necessary harvester housekeeping
        self._refresh_harvest_objects(harvest_object, package_id)

        # Finish up
        if status == 'unchanged':
            return 'unchanged'
        else:
            log.debug('Package {} was successully harvested.'
                      .format(package['id']))
            return True
