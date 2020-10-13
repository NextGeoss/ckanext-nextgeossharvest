# -*- coding: utf-8 -*-

import logging
import json

from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class NoaGroundsegmentBaseHarvester(HarvesterBase):
    
    def _parse_content(self, entry):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        item = {}
        
        content = json.loads(entry)
        
        # Parse products by Instrument
        # Different Instruments have different filename formats
        # and filetypes so they have to be parsed in different ways
        if content['instrument'] == 'VIIRS':
            item = self._parse_viirs(content)
        elif content['instrument'] == 'MODIS':
            item = self._parse_modis(content)
        elif content['instrument'] == 'AIRS':
            item = self._parse_airs(content)
        elif content['instrument'] == 'AVHRR-3':
            item = self._parse_avhrr(content)
        elif content['instrument'] == 'MERSI':
            item = self._parse_mersi(content)

        return item


    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        return parsed_content['resource']

    def _parse_viirs(self, content):
        item = {}

        item['name'] = content['filename'][:-3].lower() #Remove the .h5 filetype and lowercase
        item['title'] = item['name']                    #content['filename'].split('_')[0]
        item['notes'] = "Visible Infrared Imaging Radiometer Suite (VIIRS) products collected from the premises of the National Observatory of Athens (NOA). \
            For specific product information please consult the following link: https://groundsegment.space.noa.gr/products"

        item['spatial'] = content['spatial']

        item['InstrumentFamilyName'] = 'VIIRS'
        item['InstrumentName'] = 'Visible Infrared Imaging Radiometer Suite'
        item['PlatformSerialIdentifier'] = content["satellite"]
        
        item['tags'] = [{"name": "noa_groundsegment"},
                        {"name":content["instrument"].lower()},
                        {"name":content["satellite"].lower()}]

        item['identifier'] = item['name']
        
        # Product resource only
        item['resource'] = [{'name': content['filename'][:-3],
                    'description': "Download the product from NOA Groundsegment. NOTE: DOWNLOAD REQUIRES LOGIN",
                    'url': "https://groundsegment.space.noa.gr/download?reception_id={}&filename={}".format(content["reception_id"], content['filename']),
                    'format': "H5",
                    'mimetype': "H5"}]

        item['timerange_start'] = str(content['sensing_start']) + ".000Z"
        item['timerange_end'] = str(content['sensing_stop']) + ".000Z"

        item['collection_id'] = 'noa_viirs_products'
        item['collection_name'] = 'NOA VIIRS Products'  # noqa: E501
        item['collection_description'] = 'Visible Infrared Imaging Radiometer Suite (VIIRS) products collected from the premises of \
        the National Observatory of Athens (NOA).'  # noqa: E501

        return item

    def _parse_modis(self, content):
        item = {}

        name = content['filename'][:-4].lower().replace('.', '_') #Remove the .hdf filetype and lowercase #Remove the .hdf filetype and lowercase

        item['name'] = name
        item['title'] = name        #name.split('_')[0]
        item['notes'] = "Moderate Resolution Imaging Spectroradiometer (MODIS) products collected from the premises of the National Observatory of Athens (NOA). \
            For specific product information please consult the following link: https://groundsegment.space.noa.gr/products"

        item['spatial'] = content['spatial']

        item['InstrumentFamilyName'] = 'MODIS'
        item['InstrumentName'] = 'Moderate Resolution Imaging Spectroradiometer'
        item['PlatformSerialIdentifier'] = content["satellite"]
        
        item['tags'] = [{"name": "noa_groundsegment"},
                        {"name":content["instrument"].lower()},
                        {"name":content["satellite"].lower()}]

        item['identifier'] = name
        
        # Product resource only
        item['resource'] = [{'name': name,
                    'description': "Download the product from NOA Groundsegment. NOTE: DOWNLOAD REQUIRES LOGIN",
                    'url': "https://groundsegment.space.noa.gr/download?reception_id={}&filename={}".format(content["reception_id"], content['filename']),
                    'format': "HDF",
                    'mimetype': "HDF"}]

        item['timerange_start'] = str(content['sensing_start']) + ".000Z"
        item['timerange_end'] = str(content['sensing_stop']) + ".000Z"

        item['collection_id'] = 'noa_modis_products'
        item['collection_name'] = 'NOA MODIS Products'  # noqa: E501
        item['collection_description'] = 'Moderate Resolution Imaging Spectroradiometer (MODIS) products collected from the premises of \
        the National Observatory of Athens (NOA).'  # noqa: E501

        return item

    def _parse_airs(self, content):
        item = {}

        if content['filename'].endswith('.hdf'):
            name = content['filename'][:-4].lower().replace('.', '_')
            file_format = "HDF"
        else:
            name = content['filename'][:-3].lower().replace('.', '_')
            file_format = "H5"

        item['name'] = name
        item['title'] = name        #name.split('_')[0]
        item['notes'] = "Atmospheric InfraRed Sounder (AIRS) products collected from the premises of the National Observatory of Athens (NOA). \
            For specific product information please consult the following link: https://groundsegment.space.noa.gr/products"

        item['spatial'] = content['spatial']

        item['InstrumentFamilyName'] = 'AIRS'
        item['InstrumentName'] = 'Atmospheric InfraRed Sounder'
        item['PlatformSerialIdentifier'] = content["satellite"]
        
        item['tags'] = [{"name": "noa_groundsegment"},
                        {"name":content["instrument"].lower()},
                        {"name":content["satellite"].lower()}]

        item['identifier'] = name
        
        # Product resource only
        item['resource'] = [{'name': name,
                    'description': "Download the product from NOA Groundsegment. NOTE: DOWNLOAD REQUIRES LOGIN",
                    'url': "https://groundsegment.space.noa.gr/download?reception_id={}&filename={}".format(content["reception_id"], content['filename']),
                    'format': file_format,
                    'mimetype': file_format}]

        item['timerange_start'] = str(content['sensing_start']) + ".000Z"
        item['timerange_end'] = str(content['sensing_stop']) + ".000Z"

        item['collection_id'] = 'noa_airs_products'
        item['collection_name'] = 'NOA AIRS Products'  # noqa: E501
        item['collection_description'] = 'Atmospheric InfraRed Sounder (AIRS) products collected from the premises of \
        the National Observatory of Athens (NOA).'  # noqa: E501

        return item

    def _parse_avhrr(self, content):
        item = {}

        name = content['filename'][:-4].lower().replace('.', '_') #Remove the .hdf filetype and lowercase

        item['name'] = name
        item['title'] = name        #name.split('_')[0] + name.split('_')[1]
        item['notes'] = "Advanced Very High Resolution Radiometer (AVHRR/3) products collected from the premises of the National Observatory of Athens (NOA). \
            For specific product information please consult the following link: https://groundsegment.space.noa.gr/products"

        item['spatial'] = content['spatial']

        item['InstrumentFamilyName'] = 'AVHRR/3'
        item['InstrumentName'] = 'Advanced Very High Resolution Radiometer'
        item['PlatformSerialIdentifier'] = content["satellite"]
        
        item['tags'] = [{"name": "noa_groundsegment"},
                        {"name":content["instrument"].lower()},
                        {"name":content["satellite"].lower()}]

        item['identifier'] = name
        
        # Product resource only
        item['resource'] = [{'name': name,
                    'description': "Download the product from NOA Groundsegment. NOTE: DOWNLOAD REQUIRES LOGIN",
                    'url': "https://groundsegment.space.noa.gr/download?reception_id={}&filename={}".format(content["reception_id"], content['filename']),
                    'format': "HDF",
                    'mimetype': "HDF"}]

        item['timerange_start'] = str(content['sensing_start']) + ".000Z"
        item['timerange_end'] = str(content['sensing_stop']) + ".000Z"

        item['collection_id'] = 'noa_avhrr_products'
        item['collection_name'] = 'NOA AVHRR/3 Products'  # noqa: E501
        item['collection_description'] = 'Advanced Very High Resolution Radiometer (AVHRR/3) products collected from the premises of \
        the National Observatory of Athens (NOA).'  # noqa: E501

        return item

    def _parse_mersi(self, content):
        item = {}

        name = content['filename'][:-3].lower().replace('.', '_') #Remove the .hdf filetype and lowercase

        item['name'] = name
        item['title'] = name            #content['filename'][:-3].split('.')[-1]
        item['notes'] = "MEdium Resolution Spectral Imager (MERSI) products collected from the premises of the National Observatory of Athens (NOA). \
            For specific product information please consult the following link: https://groundsegment.space.noa.gr/products"

        item['spatial'] = content['spatial']

        item['InstrumentFamilyName'] = 'MERSI'
        item['InstrumentName'] = 'MEdium Resolution Spectral Imager'
        item['PlatformSerialIdentifier'] = content["satellite"]
        
        item['tags'] = [{"name": "noa_groundsegment"},
                        {"name":content["instrument"].lower()},
                        {"name":content["satellite"].lower()}]

        item['identifier'] = name
        
        # Product resource only
        item['resource'] = [{'name': name,
                    'description': "Download the product from NOA Groundsegment. NOTE: DOWNLOAD REQUIRES LOGIN",
                    'url': "https://groundsegment.space.noa.gr/download?reception_id={}&filename={}".format(content["reception_id"], content['filename']),
                    'format': "H5",
                    'mimetype': "H5"}]

        item['timerange_start'] = str(content['sensing_start']) + ".000Z"
        item['timerange_end'] = str(content['sensing_stop']) + ".000Z"

        item['collection_id'] = 'noa_mersi_products'
        item['collection_name'] = 'NOA MERSI Products'  # noqa: E501
        item['collection_description'] = 'MEdium Resolution Spectral Imager (MERSI) products collected from the premises of \
        the National Observatory of Athens (NOA).'  # noqa: E501

        return item