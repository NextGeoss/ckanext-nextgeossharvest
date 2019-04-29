# -*- coding: utf-8 -*-
import json
import logging
from datetime import datetime, timedelta

from ftplib import FTP

from ckan.plugins.core import implements

from ckan.model import Session
from ckan.model import Package

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.nextgeossharvest.lib.deimosimg_base import DEIMOSIMGBase
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester

from ckanext.harvest.model import HarvestObjectExtra as HOExtra

import base64
import requests
from bs4 import BeautifulSoup as Soup

log = logging.getLogger(__name__)


class DEIMOSIMGHarvester(NextGEOSSHarvester, DEIMOSIMGBase):
    '''
    A Harvester for DEIMOS2 Products.
    '''
    implements(IHarvester)

    def __init__(self, *args, **kwargs):
        super(type(self), self).__init__(*args, **kwargs)
        self.overlap = timedelta(days=30)
        self.interval = timedelta(days=3 * 30)

    def info(self):
        return {
            'name': 'deimos2',
            'title': 'DEIMOS Imaging',
            'description': 'A Harvester for DEIMOS2 Products'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if config_obj.get('harvester_type') not in {'deimos_imaging'}:
                raise ValueError('harvester type is required and must be "deimos_imaging"')  # noqa: E501

            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                if not isinstance(timeout, int) and not timeout > 0:
                    raise ValueError('timeout must be a positive integer')

            if type(config_obj.get('password', None)) != unicode:
                raise ValueError('password is required and must be a string')

            if type(config_obj.get('username', None)) != unicode:
                raise ValueError('username is requred and must be a string')

            if type(config_obj.get('make_private', False)) != bool:
                raise ValueError('make_private must be true or false')

        except ValueError as e:
            raise e

        return config

    def _normalize_names(self, item_node):
        """
        Return a dictionary of metadata fields with normalized names.

        The CRM entries are composed of metadata elements with names
        corresponding to the contents of name_elements and title, link,
        etc. elements. We can just extract all the metadata elements at
        once and rename them in one go.
        """
        normalized_names = {
            'ms:image_date': 'StartTime',
            'gml:poslist': 'spatial',
            'ms:cloudpercent': 'CloudCoverage',
            'ms:roll_filter': 'Roll',
            'ms:pitch_filter': 'Pitch',
            'ms:yaw_filter': 'Yaw',
            'ms:asceid': 'ASCID'
        }

        item = {}
        for subitem_node in item_node.findChildren():
            if subitem_node.name in normalized_names:
                key = normalized_names[subitem_node.name]
                if key:
                    item[key] = subitem_node.text

        return item

    def _get_metadata(self, raw_identifier):
        metadata = {}
        base_url = ('http://extcat.deimos-imaging.com/cgi-bin'
                    '/mapwfs-ext-2018/?service=WFS&version=1.1.0' +
                    '&request=GetFeature&typename=DE2&srsname=EPSG:4326' +
                    '&filter=<ogc:Filter+xmlns:ogc=' +
                    '"http://www.opengis.net/ogc"><ogc:PropertyIsEqualTo>' +
                    '<ogc:PropertyName>image_name</ogc:PropertyName>' +
                    '<ogc:Literal>{}</ogc:Literal></ogc:PropertyIsEqualTo>' +
                    '</ogc:Filter> '
                    )

        metadata_url = base_url.format(raw_identifier)

        try:
            r = requests.get(metadata_url)
        except Exception:
            return metadata

        soup = Soup(r.content, 'lxml')
        metadata = self._normalize_names(soup)
        return metadata

    def gather_stage(self, harvest_job):
        self.log = logging.getLogger(__file__)
        self.log.debug('DEIMOS2 Harvester gather_stage for job: %r', harvest_job)  # noqa: E501
        config = self._get_config(harvest_job)

        ids = self._gather(harvest_job, config)
        return ids

    def _gather(self, job, config):

        ftp_user = config['username']
        ftp_passwd = config['password']
        source_type = config['harvester_type']
        ftp_source = create_ftp_source(source_type)

        if not hasattr(self, 'harvester_logger'):
            self.harvester_logger = self.make_harvester_logger()

        self.provider = 'deimos_imaging'

        existing_files = ftp_source._get_ftp_urls(ftp_user, ftp_passwd)

        metadata_dict = {}
        ids = []
        new_counter = 0
        for ftp_url in existing_files:
            filename = self.parse_filename(ftp_url)
            product_type = self.parse_filedirectory(ftp_url)
            identifier = filename

            content = {'identifier': identifier, 'product_type': product_type, 'ftp_link': ftp_url}  # noqa: E501

            raw_id = identifier.replace(product_type, 'L0R')

            if raw_id in metadata_dict:
                metadata = metadata_dict[raw_id]
            else:
                metadata = self._get_metadata(raw_id)
                metadata_dict[raw_id] = metadata

            for key in metadata:
                content[key] = metadata[key]

            content = json.dumps(content, default=str)

            package = Session.query(Package) \
                .filter(Package.name == identifier.lower()).first()

            if package:
                log.debug('{} will not be updated.'.format(identifier))  # noqa: E501
                status = 'unchanged'
                obj = HarvestObject(guid=ftp_url, job=job,
                                    extras=[HOExtra(key='status',
                                            value=status)])

                obj.content = content
                obj.package = package
                obj.save()
                ids.append(obj.id)

            elif not package:
                log.debug('{} has not been harvested before. Creating a new harvest object.'.format(identifier))  # noqa: E501
                status = 'new'
                new_counter += 1

                extras = [HOExtra(key='status', value=status)]

                obj = HarvestObject(job=job,
                                    guid=ftp_url,
                                    extras=extras)

                obj.content = content
                obj.package = None
                obj.save()
                ids.append(obj.id)

        harvester_msg = '{:<12} | {} | Job ID:{} | {} | {}'
        if hasattr(self, 'harvester_logger'):
            timestamp = str(datetime.utcnow())
            self.harvester_logger.info(harvester_msg.format(self.provider,
                timestamp, job.id, new_counter, '0'))  # noqa: E128, E501

        return ids

    def fetch_stage(self, harvest_object):
        return True

    def _get_config(self, harvest_job):
        return json.loads(harvest_job.source.config)


def create_ftp_source(source_type):
    return FtpSource(**FTP_SOURCE_CONF[source_type])


class FtpSource(object):

    def __init__(self, domain, directories):
        self.domain = domain
        self.directories = directories

    def _get_ftp_urls(self, user, passwd):
        ftp_urls = set()
        ftp = FTP(self._get_ftp_domain(), user, passwd)
        for directory in self._get_ftp_directories():
            ftp.cwd('/{}'.format(directory))
            ftp_urls |= set(self._ftp_url(directory, fname, user) for fname in ftp.nlst())  # noqa: E501
        return ftp_urls

    def _ftp_url(self, directory, filename, ftp_user):
        base_url = '{}/descarga.php'.format(self._get_ftp_domain())
        b64_file = base64.b64encode(filename)
        file_url = 'file={}'.format(b64_file)
        directory = '{}\{}'.format(ftp_user, directory)
        b64_dir = base64.b64encode(directory)
        dir_url = 'dir={}'.format(b64_dir)

        ftp_url = '{}?{}&{}'.format(base_url, file_url, dir_url)

        return ftp_url

    def _get_ftp_domain(self):
        return self.domain

    def _get_ftp_directories(self):
        return self.directories

# Configuration for the harvester providers:
# '<harvester_type as in config>': {
#   'domain': url domain of the source,
#   'directories': list that details all the directories
# }
#        'directories': ['PM4_L1B', 'PSH_L1B', 'PSH_L1C']


FTP_SOURCE_CONF = {
    'deimos_imaging': {
        'domain': 'ftp.deimos-imaging.com',
        'directories': ['PM4_L1B', 'PSH_L1B', 'PSH_L1C']
    }
}
