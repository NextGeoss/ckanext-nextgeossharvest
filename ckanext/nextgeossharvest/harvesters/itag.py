# -*- coding: utf-8 -*-

import logging
import json
import time

from shapely.geometry import Polygon
import requests
from requests.exceptions import Timeout
import jmespath
from sqlalchemy.sql import update, bindparam

from ckan import model
from ckan import logic
from ckan.logic import ValidationError
from ckan.lib.navl.validators import not_empty
from ckan.plugins.core import implements

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.interfaces import IHarvester

from ckanext.nextgeossharvest.lib.esa_base import SentinelHarvester
from ckanext.nextgeossharvest.lib.opensearch_base import OpenSearchHarvester
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester


class ITagEnricher(SentinelHarvester, OpenSearchHarvester, NextGEOSSHarvester):
    """
    A metadata enricher that uses iTag to obtain additional metadata.
    """
    implements(IHarvester)

    def info(self):
        return {
            'name': 'itag_enricher',
            'title': 'iTag Metadata Enricher',
            'description': 'A metadata enricher that uses iTag to obtain additional metadata'  # noqa: E501
        }

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.ITagEnricher.gather')
        log.debug('ITagEnricher gather_stage for job: %r', harvest_job)

        # Save a reference
        self.job = harvest_job

        context = {'model': model,
                   'session': model.Session,
                   'user': self._get_user_name()}

        org_id = model.Package.get(harvest_job.source.id).owner_org
        organization = logic.get_action('organization_show')(context, {'id': org_id})  # noqa: E501

        # Exclude Sentinel-3 because it seems like iTag can't handle the curved
        # footprints.
        filter_query = '+organization:{} -itag:tagged -FamilyName:Sentinel-3'.format(organization['name'])  # noqa: E501

        ids = []

        # We'll limit this to 100 datasets per job so that results appear
        # faster
        start = 0
        rows = 100
        untagged = logic.get_action('package_search')(context,
                                                      {'fq': filter_query,
                                                       'rows': rows,
                                                       'start': start})
        results = untagged['results']
        for result in results:
            spatial = None
            for i in result['extras']:
                if i['key'] == 'spatial':
                    spatial = i['value']
            if spatial:
                obj = HarvestObject(guid=result['id'], job=self.job,
                                    extras=[HOExtra(key='status', value='change'),  # noqa: E501
                                            HOExtra(key='spatial', value=spatial),  # noqa: E501
                                            HOExtra(key='package', value=json.dumps(result))])  # noqa: E501
                obj.save()
                ids.append(obj.id)

        return ids

    def fetch_stage(self, harvest_object):
        log = logging.getLogger(__name__ + '.fetch')
        log.debug('Starting iTag fetch for package {}'
                  .format(harvest_object.id))

        # Limit requests to one per second so the server doesn't fall over.
        start_request = time.time()

        template = '{}/?taggers={}&_pretty=true&footprint={}'
        base_url = 'http://itag-nextgeoss.deimos.pt/itag'
        taggers = 'Political,Geology,Hydrology,LandCover2009'
        spatial = json.loads(self._get_object_extra(harvest_object, 'spatial'))
        coords = Polygon([(x[0], x[1]) for x in spatial['coordinates'][0]]).wkt
        query = template.format(base_url, taggers, coords)
        try:
            r = requests.get(query, timeout=6)
            assert r.status_code == 200
            response = r.text
        except AssertionError as e:
            self._save_object_error('{} error on request: {}'
                                    .format(r.status_code, r.text),
                                    harvest_object, 'Fetch')
            # TODO: There should be a way to limit the fetch process itself
            # to one request per second or similar. ###########################
            end_request = time.time()
            request_time = end_request - start_request
            if request_time < 1.0:
                time.sleep(1 - request_time)
            # End TODO ########################################################
            return False
        except Timeout as e:
            self._save_object_error('Request timed out: {}'
                                    .format(e),
                                    harvest_object, 'Fetch')
            end_request = time.time()
            request_time = end_request - start_request
            if request_time < 1.0:
                time.sleep(1 - request_time)
            return False
        except Exception as e:
            message = e.message
            if not message:
                message = repr(e)
            self._save_object_error('Error fetching: {}'
                                    .format(message),
                                    harvest_object, 'Fetch')
            end_request = time.time()
            request_time = end_request - start_request
            if request_time < 1.0:
                time.sleep(1 - request_time)
            return False

        harvest_object.content = response
        harvest_object.save()

        end_request = time.time()
        request_time = end_request - start_request
        if request_time < 1.0:
            time.sleep(1 - request_time)

        return True

    def import_stage(self, harvest_object):
        log = logging.getLogger(__name__ + '.import')
        log.debug('Import stage for package {}'
                  .format(harvest_object.id))

        self.obj = harvest_object  # Remove later
        if harvest_object.content is None:
            self._save_object_error('Empty content for object {}'
                                    .format(harvest_object.id),
                                    harvest_object, 'Import')
            return False

        package = json.loads(self._get_object_extra(harvest_object, 'package'))

        content = json.loads(harvest_object.content)['content']
        itag_tags = self._get_itag_tags(content)
        itag_extras = self._get_itag_extras(content)

        # Include an itag: tagged extra, even if there are no new tags or
        # extras, so that we can differentiate between datasets that we've
        # tried to tag and datasets that we haven't tried to tag.
        itag_extras.append({'key': 'itag', 'value': 'tagged'})

        package['tags'] = self._update_tags(package['tags'], itag_tags)
        package['extras'] = self._update_extras(package['extras'], itag_extras)

        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
        }
        package_schema = logic.schema.default_update_package_schema()
        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty, unicode]
        extras_schema = logic.schema.default_extras_schema()
        package_schema['tags'] = tag_schema
        package_schema['extras'] = extras_schema
        context['schema'] = package_schema

        try:
            package = logic.get_action('package_update')(context, package)
        except ValidationError as e:
            self._save_object_error('Error updating {}: {}'
                                    .format(package['name'], e.message),
                                    harvest_object, 'Import')
            return False

        # Flag the other objects of this source as not current anymore
        from ckanext.harvest.model import harvest_object_table
        u = update(harvest_object_table) \
            .where(harvest_object_table.c.package_id == bindparam('pkg_id')) \
            .values(current=False)
        model.Session.execute(u, params={'pkg_id': package['id']})
        model.Session.commit()
        # Refresh current object from session, otherwise the
        # import paster command fails
        # (Copied from the Gemini harvester--not sure if necessary)
        self.obj.content = str(self.obj.content)
        model.Session.remove()
        model.Session.add(self.obj)
        model.Session.refresh(self.obj)

        # Set reference to package in the HarvestObject and flag it as
        # the current one
        if not self.obj.package_id:
            self.obj.package_id = package['id']

        self.obj.current = True
        self.obj.save()

        return True

    def _get_itag_tags(self, content):
        """Return a list of all iTag tags (may be an empty list)"""
        continents = jmespath.search('political.continents[*].name', content) or []  # noqa: E501
        countries = jmespath.search('political.continents[*].countries[].name', content) or []  # noqa: E501
        regions = jmespath.search('political.continents[*].countries[].regions[].name', content) or []  # noqa: E501
        states = jmespath.search('political.continents[*].countries[].regions[].states[].name', content) or []  # noqa: E501
        toponyms = jmespath.search('political.continents[*].countries[].regions[].states[].toponyms[].name', content) or []  # noqa: E501
        geologies = jmespath.search('geology.*[].name', content) or []
        # Hydrologies includes rivers, which should be renamed
        rivers = jmespath.search('hydrology.rivers[].name', content) or []
        rivers = [u'{} River'.format(x) for x in rivers if x]
        non_rivers = jmespath.search('hydrology.[!rivers][].name', content) or []  # noqa: E501
        hydrologies = rivers + non_rivers
        land_use = jmespath.search('landCover.landUse[].name', content) or []

        # Combine all the lists and remove any that are empty or None
        itag_names = list(set(continents + countries + regions + states +
                              toponyms + geologies + hydrologies + land_use))

        itag_tags = [{'name': name} for name in itag_names]

        return itag_tags

    def _get_itag_extras(self, content):
        """Return a list of all iTag extras (may be an empty list)."""
        land_cover = jmespath.search('landCover.landUse[].[name, pcover]', content) or []  # noqa: E501

        # Combine the lists to extra dicts and remove any with missing data
        # Since we don't have a schema, we'll combine this list into one big
        # extra to avoid creating confusing metadata. It seems like this
        # should be a field with subfields in the future.
        land_cover_extra = str([{'key': x[0], 'value': x[1]} for x
                                in land_cover if x[0] and x[1]])

        itag_extras = [{'key': 'Land Cover', 'value': land_cover_extra}]

        return itag_extras
