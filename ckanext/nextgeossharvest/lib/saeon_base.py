# -*- coding: utf-8 -*-

from ckanext.harvest.harvesters.base import HarvesterBase
from ckan import model
from ckan.model import Package
from ckan.model import Session
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from datetime import datetime
from string import Template
from bs4 import BeautifulSoup as Soup
import logging
import requests
from requests.exceptions import Timeout
import time
import uuid
import re


log = logging.getLogger(__name__)


class CSAGHarvester(HarvesterBase):

    def _normalize_names(self, item_node):
        """
        Return a dictionary of metadata fields with normalized names.

        The Sentinel entries are composed of metadata elements with names
        corresponding to the contents of name_elements and title, link,
        etc. elements. We can just extract all the metadata elements at
        once and rename them in one go.

        Note that elements like `ingestiondate`, which are included in the
        scihub results, will not be added to item as they are not part of the
        list of elements added in the original version.
        """

        spatial_dict = {'ows:lowercorner': None,
                        'ows:uppercorner': None}

        # Since Saeon Catalogue datasets refer to an entire year,
        # there is only one datastamp. Thus, this value will be
        # added to both timerange_start and timerange_end, and later
        # pos-processed
        normalized_names = {
            'dct:modified': 'timerange_start',
            'ows:lowercorner': 'spatial',
            'ows:uppercorner': 'spatial',
            'dc:identifier': 'identifier',
            'dc:title': 'title',
            'dc:type': 'type',
            'dct:abstract': 'notes'
        }
        item = {'spatial': spatial_dict}

        for subitem_node in item_node.findChildren():
            if subitem_node.name in normalized_names:
                key = normalized_names[subitem_node.name]
                if key:
                    if key == 'spatial':
                        item[key][subitem_node.name] = subitem_node.text
                    else:
                        item[key] = subitem_node.text

        # Since the spatial field is composed by 2 values,
        # if either of the values is None, then the whole field is dismissed
        for key in item['spatial']:
            if item['spatial'][key] is None:
                del item['spatial']
                break

        if ('timerange_start' in item) and ('timerange_end' not in item):
            item['publication_date'] = item['timerange_start']
            item['timerange_end'] = item['timerange_start']
            item['timerange_start'] += '-01-01T00:00:00.000Z'
            item['timerange_end'] += '-12-31T23:59:59.999Z'

        return item

    def _add_collection(self, item):
        """Return the item with collection ID, name, and description."""
        item['collection_name'] = 'Climate Systems Analysis Group (South Africa)'  # noqa: E501
        item['collection_description'] = 'A collection of datasets produced by the Climate Systems Analysis Group, University of Cape Town. The data were harvested through the SAEON Open Data Platform. The collection contains model runs produced by downscaling Global Climate Models (GCMs) using the Self-Organising Map (SOM) technique. SOM is a leading empirical downscaled technique and provides meteorological station level response to global climate change forcing. The SOM technique was employed to project rainfall and temperature changes for 1950-1999 (current climate), 2046–2065 (near future) and 2080–2100 (far future) periods for South Africa.'  # noqa: E501
        item['collection_id'] = 'CLIMATE_SYSTEMS_ANALYSIS_GROUP_SOUTH_AFRICA'

        return item

    def _get_tags_for_dataset(self, tags_list):
        """Creates a list of tag dictionaries based on a product's metadata."""
        tags = [{'name': 'south africa'}, {'name': 'africa'}]
        for tag in tags_list:
            tags.append({'name': tag.text})

        return tags

    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        soup = Soup(content, 'lxml')

        # Create an item dictionary and add metadata with normalized names.
        item = self._normalize_names(soup)

        # If there's a spatial element, convert it to GeoJSON
        # Remove it if it's invalid
        spatial_data = item.pop('spatial', None)
        if spatial_data:
            template = Template('''{"type": "Polygon", "coordinates": [$coords_list]}''')  # noqa: E501
            spatial_data['gmd:westboundlongitude'], spatial_data['gmd:northboundlatitude'] = spatial_data['ows:lowercorner'].split(' ')
            spatial_data['gmd:eastboundlongitude'], spatial_data['gmd:southboundlatitude'] = spatial_data['ows:uppercorner'].split(' ')
            coords_NW = [float(spatial_data['gmd:westboundlongitude']), float(spatial_data['gmd:northboundlatitude'])]  # noqa: E501
            coords_NE = [float(spatial_data['gmd:eastboundlongitude']), float(spatial_data['gmd:northboundlatitude'])]  # noqa: E501
            coords_SE = [float(spatial_data['gmd:eastboundlongitude']), float(spatial_data['gmd:southboundlatitude'])]  # noqa: E501
            coords_SW = [float(spatial_data['gmd:westboundlongitude']), float(spatial_data['gmd:southboundlatitude'])]  # noqa: E501
            coords_list = [coords_NW, coords_NE, coords_SE, coords_SW, coords_NW]  # noqa: E501

            geojson = template.substitute(coords_list=coords_list)
            item['spatial'] = geojson

        name = item['identifier'].lower()
        item['name'] = 'saeon_csag_' + name.replace('.', '_').replace('/', '-')

        resources = []
        # Thumbnail, alternative and enclosure

        resources_list = soup.find_all({'dct:references'})
        for resource in resources_list:
            if resource['scheme'] == 'Query':
                resources.append(self._make_thumbnail_resource(resource.text))
            elif resource['scheme'] == 'Information':
                resources.append(self._make_information_resource(resource.text))
            elif resource['scheme'] == 'Download':
                resources.append(self._make_dataset_link(resource.text))

        # Add the collection info
        item = self._add_collection(item)

        tags_list = soup.find_all({'dc:subject'})
        item['tags'] = self._get_tags_for_dataset(tags_list)
        item['resource'] = resources

        return item

    def _make_information_resource(self, url):
        """
        Return an information resource dictionary
        """
        name = 'Information from SAEON'
        description = 'Access information link fro SAEON.'  # noqa: E501
        _type = 'saeon_information'

        information = {'name': name,
                   'description': description,
                   'url': url,
                   'format': 'HTML',
                   'mimetype': 'text/html',
                   'resource_type': _type}

        return information

    def _make_thumbnail_resource(self, url):
        """
        Return a thumbnail resource dictionary
        """
        name = 'Thumbnail Download'
        description = 'Download a PNG quicklook.'  # noqa: E501
        _type = 'thumbnail'

        thumbnail = {'name': name,
                     'description': description,
                     'url': url.replace('application/openlayers','image/png&transparent=true'),
                     'format': 'PNG',
                     'mimetype': 'image/png',
                     'resource_type': _type}

        return thumbnail

    def _make_dataset_link(self, url):
        """
        Return a dataset resource dictionary
        """
        name = 'Product Download'
        description = 'Download a ZIP fIle.'
        
        _type = 'saeon_product'

        dataset = {'name': name,
                   'description': description,
                   'url': url,
                   'format': 'ZIP',
                   'mimetype': 'application/zip',
                   'resource_type': _type}

        return dataset

    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        if self.obj.package and self.obj.package.resources:
            old_resources = [x.as_dict() for x in self.obj.package.resources]
        else:
            old_resources = None

        new_resources = parsed_content['resource']
        if not old_resources:
            resources = new_resources
        else:
            # Replace existing resources or add new ones
            new_resource_types = {x['resource_type'] for x in new_resources}
            resources = []
            for old in old_resources:
                old_type = old.get('resource_type')
                if old_type not in new_resource_types:
                    resources.append(old)
            resources += new_resources

        resources.sort(key=lambda x: x['name'])

        return resources

    def _get_entries_from_results(self, soup, restart_record, next_record):
        """Extract the entries from an CSWSearch response."""
        entries = []

        for entry in soup.find_all({'csw:summaryrecord'}):
            content = entry.encode()
            # The lowercase identifier will serve as the dataset's name,
            # so we need the lowercase version for the lookup in the next step.
            identifier = entry.find('dc:identifier').text.lower()  # noqa: E501
            identifier = 'saeon_csag_' + identifier.replace('.', '_').replace('/', '-')
            guid = unicode(uuid.uuid4())

            entries.append({'content': content, 'identifier': identifier, 'guid': guid, 'restart_record': restart_record})  # noqa: E501

        # If this job is interrupted mid-way, then the new job will re-harvest
        # the collections of this job (restart_record is the initial record)
        # If the job is finished (gone through all the entries), then the new
        # job will harvest new collections (restart_record is the next record)
        if len(entries) > 1:
            entries[-1]['restart_record'] = next_record

        return entries

    def _get_next_url(self, harvest_url, records_returned, next_record, limit):
        """
        Get the next URL.

        Return None of there is none next URL (end of results).
        """
        if next_record != '0' and eval(records_returned) == limit:
            splitted_url = harvest_url.split('StartPosition')
            next_url = splitted_url[0] + 'StartPosition=' + next_record
            return next_url
        else:
            return None

    def _crawl_results(self, harvest_url, limit=100, timeout=5):  # noqa: E501
        """
        Iterate through the results, create harvest objects,
        and return the ids.
        """
        ids = []
        new_counter = 0
        update_counter = 0

        while len(ids) < limit and harvest_url:
            # We'll limit ourselves to one request per second
            start_request = time.time()

            # Make a request to the website
            timestamp = str(datetime.utcnow())
            log_message = '{:<12} | {} | {} | {}s'
            try:
                r = requests.get(harvest_url, timeout=timeout)
            except Timeout as e:
                self._save_gather_error('Request timed out: {}'.format(e), self.job)  # noqa: E501
                status_code = 408
                elapsed = 9999
                if hasattr(self, 'provider_logger'):
                    self.provider_logger.info(log_message.format(self.provider,
                        timestamp, status_code, timeout))  # noqa: E128
                return ids
            if r.status_code != 200:
                self._save_gather_error('{} error: {}'.format(r.status_code, r.text), self.job)  # noqa: E501
                elapsed = 9999
                if hasattr(self, 'provider_logger'):
                    self.provider_logger.info(log_message.format(self.provider,
                        timestamp, r.status_code, elapsed))  # noqa: E128
                return ids

            if hasattr(self, 'provider_logger'):
                self.provider_logger.info(log_message.format(self.provider,
                    timestamp, r.status_code, r.elapsed.total_seconds()))  # noqa: E128, E501

            soup = Soup(r.content, 'lxml')

            next_url = soup.find('csw:searchresults', elementset="summary")
            records_returned = next_url['numberofrecordsreturned']
            next_record = next_url['nextrecord']
            number_records_matched = next_url['numberofrecordsmatched']

            if next_record != '0':
                current_record = str(eval(next_record) - eval(records_returned))  # noqa: E501
            else:
                current_record = str(eval(number_records_matched) - eval(records_returned))  # noqa: E501

            # Get the URL for the next loop, or None to break the loop
            # Only works if StartPosition is last URL parameter
            harvest_url = self._get_next_url(harvest_url, records_returned, next_record, limit)  # noqa: E501

            # Get the entries from the results
            entries = self._get_entries_from_results(soup, current_record, next_record)  # noqa: E501

            # Create a harvest object for each entry
            for entry in entries:
                entry_guid = entry['guid']
                entry_name = entry['identifier']

                package = Session.query(Package) \
                    .filter(Package.name == entry_name).first()

                if package:
                    # Meaning we've previously harvested this,
                    # but we may want to reharvest it now.
                    previous_obj = model.Session.query(HarvestObject) \
                        .filter(HarvestObject.guid == entry_guid) \
                        .filter(HarvestObject.current == True) \
                        .first()  # noqa: E712
                    if previous_obj:
                        previous_obj.current = False
                        previous_obj.save()

                    if self.update_all:
                        log.debug('{} already exists and will be updated.'.format(entry_name))  # noqa: E501
                        status = 'change'
                        update_counter += 1
                    else:
                        log.debug('{} already exists and will not be updated.'.format(entry_name))  # noqa: E501
                        status = 'unchanged'

                    obj = HarvestObject(guid=entry_guid, job=self.job,
                                        extras=[HOExtra(key='status',
                                                value=status),
                                                HOExtra(key='restart_record',
                                                value=entry['restart_record'])])  # noqa: E501
                    obj.content = entry['content']
                    obj.package = package
                    obj.save()
                    ids.append(obj.id)
                elif not package:
                    # It's a product we haven't harvested before.
                    log.debug('{} has not been harvested before. Creating a new harvest object.'.format(entry_name))  # noqa: E501
                    obj = HarvestObject(guid=entry_guid, job=self.job,
                                        extras=[HOExtra(key='status',
                                                value='new'),
                                                HOExtra(key='restart_record',
                                                value=entry['restart_record'])])  # noqa: E501
                    new_counter += 1
                    obj.content = entry['content']
                    obj.package = None
                    obj.save()
                    ids.append(obj.id)

            end_request = time.time()
            request_time = end_request - start_request
            if request_time < 1.0:
                time.sleep(1 - request_time)

        harvester_msg = '{:<12} | {} | jobID:{} | {} | {}'
        if hasattr(self, 'harvester_logger'):
            timestamp = str(datetime.utcnow())
            self.harvester_logger.info(harvester_msg.format(self.provider,
                                       timestamp, self.job.id, new_counter, update_counter))  # noqa: E128, E501

        return ids
