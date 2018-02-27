# -*- coding: utf-8 -*-

import json
import logging

from sqlalchemy.sql import update, bindparam

from ckan import model
from ckan import logic
from ckan.model import Session

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
