# -*- coding: utf-8 -*-

import logging
import json
from datetime import timedelta, datetime
import requests
from requests.exceptions import ConnectTimeout, ReadTimeout
from dateutil.relativedelta import relativedelta
from ckan.model import Session
from ckan.model import Package
from ckanext.harvest.harvesters.base import HarvesterBase


log = logging.getLogger(__name__)


class GDACSBase(HarvesterBase):

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

    def _make_stop_time(self, start_date):
        stop_date = start_date + timedelta(days=1)
        return '{}T00:00:00.000Z'.format(stop_date.date())

    def _format_date_separed(self, date):
        day = datetime.strftime(date, '%d')
        month = datetime.strftime(date, '%m')
        year = datetime.strftime(date, '%Y')

        return day, month, year

    def _get_metadata_create_objects(self):
        time_interval = self.end_date - self.start_date

        ids = []
        for idx in range(time_interval.days):
            self.start_date = self.start_date + timedelta(days=idx)
            new_ids = self._get_products()
            ids.extend(new_ids)

        return ids

    def _get_metadata_create_objects_ftp_dir(self):
        year_month_list = self._create_months_years_list()
        ids = []
        for year, month in year_month_list:
            new_ids = self._get_products_ftp_dir(year, month)
            ids.extend(new_ids)
        return ids

    def _create_months_years_list(self):
        dates_list = list()

        current_date = self.start_date - timedelta(days=31)
        while current_date < self.end_date:
            dates_list.append((current_date.strftime('%Y'),
                               current_date.strftime('%m')))
            current_date += relativedelta(months=1)
        return dates_list

    def _create_tags(self):
        """Create a list of tags based on the type of harvester."""
        tags_list = [{"name": "GDACS"}, {"name": "GFDS"},
                     {"name": "average flood"},
                     {"name": "flood detection"},
                     {"name": "global flood detection system"},
                     {"name": "flood"}]

        if self.data_type == 'signal':
            tags_list.extend([{"name": "average flood signal"},
                              {"name": "signal"}])
        elif self.data_type == 'magnitude':
            tags_list.extend([{"name": "average flood magnitude"},
                              {"name": "magnitude"}])

        return tags_list

    # Required by NextGEOSS base harvester
    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        self.data_type = self.source_config['data_type']

        content = json.loads(content)
        http_link = content['http_link']
        start_date = deserialize_date(content['start_date'])
        start_date_string = str(start_date.date())

        spatial_template = '{{"type":"Polygon", "coordinates":[{}]}}'
        metadata = {}

        if self.data_type == 'signal':
            metadata['collection_id'] = ('AVERAGE_FLOOD_SIGNAL')
            metadata['title'] = "Average Flood Signal"
            metadata['notes'] = ("Global merged daily and 4-day average flood signal datasets between 1997 and current."  # noqa: E501
                                 " Highest sampling rate, global coverage. May have artifacts due to multi-sensor integration.")  # noqa: E501
            metadata['spatial'] = spatial_template.format([[-180, 90],
                                                           [180, 90],
                                                           [180, -90],
                                                           [-180, -90],
                                                           [-180, 90]])
            metadata['downloadLink'] = http_link

        elif self.data_type == 'magnitude':
            metadata['collection_id'] = ('AVERAGE_FLOOD_MAGNITUDE')
            metadata['title'] = "Average Flood Magnitude"
            metadata['notes'] = ("Global merged daily and 4-day average flood magnitude datasets between 1997 and current."  # noqa: E501
                                 " Highest sampling rate, global coverage. May have artifacts due to multi-sensor integration.")  # noqa: E501
            metadata['spatial'] = spatial_template.format([[-180, 90],
                                                           [180, 90],
                                                           [180, -90],
                                                           [-180, -90],
                                                           [-180, 90]])
            metadata['downloadLink'] = http_link

        # Add common metadata
        metadata['identifier'] = content['identifier']
        metadata['filename'] = metadata['identifier'] + '.tif'
        metadata['name'] = metadata['identifier'].lower()

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
        metadata['StartTime'] = '{}T00:00:00.000Z'.format(start_date_string)  # noqa E501
        metadata['StopTime'] = '{}T23:59:59.999Z'.format(start_date_string)  # noqa E501
        metadata['timerange_start'] = metadata['StartTime']
        metadata['timerange_end'] = metadata['StopTime']

        return metadata

    # Required by NextGEOSS base harvester
    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        resources = []

        resources.append(self._make_resource(metadata['downloadLink'],
                                             'Product Download'))
        return resources

    def _make_resource(self, url, name, size=None):
        """Return a resource dictionary."""
        resource_dict = {}
        resource_dict['name'] = name
        resource_dict['url'] = url
        resource_dict['format'] = 'tif'
        resource_dict['mimetype'] = 'image/tiff'
        resource_dict['description'] = ('Download the TIF from GDACS')

        return resource_dict

    def convert_date_config(self, term):
        """Convert a term into a datetime object."""
        if term == 'YESTERDAY':
            date_time = datetime.now() - timedelta(days=1)
        elif term in {'TODAY', 'NOW'}:
            date_time = datetime.now()

        return date_time.replace(hour=0, minute=0, second=0, microsecond=0)

    def _crawl_urls_http(self, url, provider):
        """Return file size after test URL"""
        timestamp = str(datetime.utcnow())
        log_message = '{:<12} | {} | {} | {}s'
        elapsed = 9999
        try:
            s = requests.Session()
            r = s.get(url)
            status_code = r.status_code
            elapsed = r.elapsed.total_seconds()
        except (ConnectTimeout, ReadTimeout) as e:
            self._save_gather_error('Request timed out: {}'.format(e), self.job)  # noqa: E501
            status_code = 408
            elapsed = 9999

        self.provider_logger.info(log_message.format(provider, timestamp,
                                                     status_code, elapsed))
        return status_code


def deserialize_date(date_string):
    """Deserialize dates serialized by calling str(date)."""
    return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
