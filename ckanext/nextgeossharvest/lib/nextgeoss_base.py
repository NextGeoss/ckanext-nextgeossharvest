# -*- coding: utf-8 -*-

import json
import logging
import uuid
from string import Template

from sqlalchemy.sql import update, bindparam
import shapely.wkt
from shapely.errors import ReadingError, WKTReadingError

from ckan import plugins as p
from ckan import model
from ckan.model import Session
from ckan.model import Package
from ckan import logic
from ckan.lib.navl.validators import not_empty

from ckanext.harvest.harvesters.base import HarvesterBase


log = logging.getLogger(__name__)


class NextGEOSSHarvester(HarvesterBase):
    """
    Base class for all NextGEOSS harvesters including helper methods and
    methods that all harvesters must call. We may want to move some of
    SentinelHarvester's methods (see esa_base.py) to this class.
    """

    def _get_object_extra(self, harvest_object, key, default=None):
        """
        Helper function for retrieving the value from a harvest object extra.
        """
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return default

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
        }

        return logic.get_action('package_show')(context, {'id': package.id})

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

    def _create_or_update_dataset(self, harvest_object, status):
        """
        Create a data dictionary and then creating or update a dataset.

        We may want to move this to the NextGEOSSHarvester base class."""
        parsed_content = self._parse_content(harvest_object.content)
        package_dict = {}
        package_dict['name'] = parsed_content['name']
        package_dict['title'] = parsed_content['title']
        package_dict['notes'] = parsed_content['notes']
        package_dict['tags'] = parsed_content['tags']
        package_dict['extras'] = self._get_extras(parsed_content)
        package_dict['extras'].append({
            'key': 'harvest_source_id',
            'value': harvest_object.harvest_source_id})
        package_dict['resources'] = self._get_resources(parsed_content)

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
            package_dict['id'] = unicode(uuid.uuid4())
            package_dict['owner_org'] = model.Package.get(harvest_object.source.id).owner_org  # noqa: E501
            package_schema = logic.schema.default_create_package_schema()
            package_schema['id'] = [unicode]
            action = 'package_create'

        # Create context after establishing if we're updating or creating
        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
        }
        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty, unicode]
        extras_schema = logic.schema.default_extras_schema()
        package_schema['tags'] = tag_schema
        package_schema['extras'] = extras_schema
        context['schema'] = package_schema

        try:
            package = p.toolkit.get_action(action)(context, package_dict)
        # IMPROVE: I think ckan.logic.ValidationError is the only Exception we
        # really need to worry about. #########################################
        except Exception as e:
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
        if coords_type != 'POLYGON':
            return None

        # Remove double coordinates -- they are not valid GeoJSON and Solr
        # will reject them.
        coords_list = [list(coords.exterior.coords[0])]
        for i in coords.exterior.coords[1:]:
            new_coord = list(i)
            if new_coord != coords_list[-1]:
                coords_list.append(new_coord)

        template = Template('''{"type": "Polygon", "coordinates": [$coords_list]}''')  # noqa: E501
        geojson = template.substitute(coords_list=coords_list)

        return geojson

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        if self.obj.package and self.obj.package.resources:
            old_resources = [x.as_dict() for x in self.obj.package.resources]
        else:
            old_resources = None

        product = self._make_product_resource(parsed_content)
        manifest = self._make_manifest_resource(parsed_content)
        thumbnail = self._make_thumbnail_resource(parsed_content)

        new_resources = [x for x in [product, manifest, thumbnail] if x]
        if not old_resources:
            resources = new_resources
        else:
            # Replace existing resources or add new ones
            new_resource_types = {x['resource_type'] for x in new_resources}
            resources = []
            for old in old_resources:
                old_type = old.get('resource_type')
                order = old.get('order')
                if old_type not in new_resource_types and order:
                    resources.append(old)
            resources += new_resources

            resources.sort(key=lambda x: x['order'])

        return resources
