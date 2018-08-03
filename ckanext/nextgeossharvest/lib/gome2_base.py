# -*- coding: utf-8 -*-

import logging
import json
from datetime import timedelta, datetime

import requests
from requests.exceptions import Timeout

from ckan.model import Session
from ckan.model import Package

from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.model import HarvestObject


log = logging.getLogger(__name__)


class GOME2Base(HarvesterBase):

    def _create_harvest_object(self, content_dict):

        extras = [HOExtra(key='status',
                          value='new')]

        # The NextGEOSS harvester flow requires content in the import stage.
        content = json.dumps(content_dict)

        obj = HarvestObject(job=self.job, guid=content_dict['identifier'],
                            extras=extras, content=content)

        obj.save()

        return obj.id

    def _create_harvest_objects(self):
        """Create harvest objects for all dates in the date range."""
        coverages = ['GOME2_O3', 'GOME2_NO2', 'GOME2_SO2', 'GOME2_SO2mass',
                     'GOME2_TropNO2']

        ids = []

        for coverage in coverages:

            content_dicts = self._content_dict_generator(coverage)
            ho_ids = [self._create_harvest_object(content_dict)
                      for content_dict in content_dicts
                      if not self._missing_or_harvested(coverage, content_dict)]  # noqa: E501
            ids.extend(ho_ids)

        return ids

    def _content_dict_generator(self, coverage):
        """Generate the minimum metadata we need to create a GOME-2 dataset."""
        content_dicts = []

        for date_string in self.date_strings:
            identifier = self._make_identifier(coverage, date_string)
            content_dicts.append({'identifier': identifier,
                                  'coverage': coverage,
                                  'date_string': date_string})
        return content_dicts

    def _missing_or_harvested(self, coverage, content_dict):
        """
        Check if a product is missing on the source server or if it's already
        been harvested. Return False if neither is the case.
        """
        if self._was_harvested(content_dict['identifier']):
            return True
        elif self._is_missing(coverage, content_dict['date_string']):
            return True
        else:
            return False

    def _was_harvested(self, identifier):
        """
        Check if a product has already been harvested and return True or False.
        """
        package = Session.query(Package) \
            .filter(Package.name == identifier.lower()).first()

        if package:
            log.debug('{} will not be updated.'.format(identifier))
            return True
        else:
            log.debug('{} has not been harvested before. Attempting to harvest it.'.format(identifier))  # noqa: E501
            return False

    def _is_missing(self, coverage, date_string):
        """Check if a product is missing on the source server."""
        url = ('https://wdc.dlr.de/data_products/VIEWER/missing_days.php?'
               'start_date={}'
               '&wpid={}').format(date_string, coverage)
        provider = 'gome-2'
        timestamp = str(datetime.utcnow())
        log_message = '{:<12} | {} | {} | {}s'

        try:
            r = requests.get(url, timeout=10)
            status_code = r.status_code
            elapsed = r.elapsed.total_seconds()

        except Timeout as e:
            self._save_gather_error('Request timed out: {}'.format(e),
                                    self.job)
            status_code = 408
            elapsed = 9999
            self.provider_logger.info(log_message.format(provider,
                                      timestamp, status_code, elapsed))
            return True

        if r.status_code != 200:
            self._save_gather_error('{} error: {}'.format(r.status_code,
                                    r.text), self.job)
            elapsed = 9999
            self.provider_logger.info(log_message.format(provider,
                                      timestamp, status_code, elapsed))
            return True

        self.provider_logger.info(log_message.format(provider, timestamp,
                                                     status_code, elapsed))

        missing_days = {x['missingDate'] for x in r.json()}

        return date_string in missing_days

    def _make_stop_time(self, start_date):
        stop_date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)  # noqa: E501
        return '{}T00:00:00.000Z'.format(stop_date.date())

    def _create_tags(self):
        """Create a list of tags based on the GOME-2 collection."""
        tags_list = [{"name": "MetOp-A"}, {"name": "GOME-2"}, {"name": "DLR"},
                     {"name": "eumetsat"}]

        if self.coverage == 'GOME2_TropNO2':
            tags_list.extend([{'name': 'Tropospheric NO2'},
                              {'name': 'Tropospheric nitrogen dioxide'},
                              {'name': 'NO2'}])

        elif self.coverage == 'GOME2_NO2':
            tags_list.extend([{'name': 'NO2'},
                              {'name': 'nitrogen dioxide'}])

        elif self.coverage == 'GOME2_O3':
            tags_list.extend([{'name': 'O3'},
                              {'name': 'ozone'}])

        elif self.coverage == 'GOME2_SO2':
            tags_list.extend([{'name': 'SO2'},
                              {'name': 'sulphur dioxide'}])

        elif self.coverage == 'GOME2_SO2mass':
            tags_list.extend([{'name': 'SO2 mass'},
                              {'name': 'sulphur dioxide mass'},
                              {'name': 'SO2'}])

        return tags_list

    # Required by NextGEOSS base harvester
    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        content = json.loads(content)

        identifier = content['identifier']
        self.coverage = content['coverage']
        self.date_string = content['date_string']

        spatial_template = '{{"type":"Polygon", "coordinates":[{}]}}'
        coordinates = [[-180, 90], [180, 90], [180, -90], [-180, -90],
                       [-180, 90]]

        metadata = {}

        description = ('Daily global concentrations of {}. These data are '
                       'crucial for monitoring atmospheric pollutants to keep '
                       'a check on the health of the Earth\'s atmosphere.')

        if self.coverage == 'GOME2_O3':
            metadata['title'] = 'MetOP-A GOME-2 Ozone (O3)'
            metadata['notes'] = description.format('atmospheric ozone')

        elif self.coverage == 'GOME2_NO2':
            metadata['title'] = 'MetOP-A GOME-2 Nitrogen Dioxide (NO2)'
            metadata['notes'] = description.format(
                'atmospheric nitrogen dioxide')

        elif self.coverage == 'GOME2_TropNO2':
            metadata['title'] = 'MetOP-A GOME-2 Tropospheric Nitrogen Dioxide (NO2)'  # noqa: E501
            metadata['notes'] = description.format(
                'tropospheric nitrogen dioxide')

        elif self.coverage == 'GOME2_SO2':
            metadata['title'] = 'MetOP-A GOME-2 Sulphur Dioxide (SO2)'
            metadata['notes'] = description.format(
                'atmospheric sulphur dioxide')

        elif self.coverage == 'GOME2_SO2mass':
            metadata['title'] = 'MetOP-A GOME-2 Sulphur Dioxide (SO2) mass'
            metadata['notes'] = description.format(
                'atmospheric sulphur dioxide mass')

        # Common metadata
        metadata['collection_id'] = 'METOP_A_{}'.format(self.coverage)
        metadata['identifier'] = identifier
        metadata['name'] = identifier.lower()
        metadata['spatial'] = spatial_template.format(coordinates)
        metadata['StartTime'] = '{}T00:00:00.000Z'.format(self.date_string)
        metadata['StopTime'] = self._make_stop_time(self.date_string)

        # For now, the collection name and description are the same as the
        # title and notes, though one or the other should probably change in
        # the future.
        metadata['collection_name'] = metadata['title']
        metadata['collection_description'] = metadata['notes']

        metadata['tags'] = self._create_tags()

        # Add time range metadata that's not tied to product-specific fields
        # like StartTime so that we can filter by a dataset's time range
        # without having to cram other kinds of temporal data into StartTime
        # and StopTime fields, etc. We do this for the Sentinel products.
        #
        # We'll want to revisit this later--it's still not clear if we can just
        # use StartTime and StopTime everywhere or if it has a special meaning
        # for certain kinds of products.
        metadata['timerange_start'] = metadata['StartTime']
        metadata['timerange_end'] = metadata['StopTime']

        return metadata

    def _make_identifier(self, coverage, date_string):
        """Return a product identifier."""
        return '{}_{}'.format(coverage, date_string)

    # Required by NextGEOSS base harvester; parsed_content isn't necessary here
    def _get_resources(self, parsed_content):
        """Return a list of resource dictionaries."""
        return [self._make_resource(resource_type)
                for resource_type in ['NetCDF3', 'GeoTIFF', 'ASCII Grid']]

    def _make_resource(self, resource_type,):
        """Return a resource dictionary."""
        resource_dict = {}
        resource_dict['name'] = 'Product Download ({})'.format(resource_type)
        if resource_type == 'NetCDF3':
            resource_dict['format'] = 'netcdf3'
            resource_dict['mimetype'] = 'application/x-netcdf3'
            file_extension = '.nc'
        elif resource_type == 'GeoTIFF':
            resource_dict['format'] = 'tiff'
            resource_dict['mimetype'] = 'application/x-tiff-32f'
            file_extension = '.tiff'
        elif resource_type == 'ASCII Grid':
            resource_dict['format'] = 'asciigrid'
            resource_dict['mimetype'] = 'application/x-asciigrid-32f'
            file_extension = '.asc'

        resource_dict['url'] = (
            'https://wdc.dlr.de/wdcservices/wcs.php?' +
            'COVERAGE=' + self.coverage +
            '&service=wcs&version=1.0.0' +
            '&crs=epsg:4326' +
            '&bbox=-180,-90,180,90' +
            '&RESX=0.25' +
            '&RESY=0.25' +
            '&request=getcoverage' +
            '&format=' + resource_dict['mimetype'] +
            '&TIME=' + self.date_string +
            '&elevation=0' +
            '&OUTPUTFILENAME=' + self.coverage +
            '_' + self.date_string + file_extension
        )

        return resource_dict

    def convert_date_config(self, term):
        """Convert a term into a datetime object."""
        if term == 'YESTERDAY':
            date_time = datetime.now() - timedelta(days=1)
        elif term in {'TODAY', 'NOW'}:
            date_time = datetime.now()

        print date_time

        return date_time.replace(hour=0, minute=0, second=0, microsecond=0)
