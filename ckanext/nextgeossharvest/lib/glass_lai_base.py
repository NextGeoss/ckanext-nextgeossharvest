# -*- coding: utf-8 -*-

import json
import uuid
import datetime
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.model import HarvestObject


class GLASS_LAI_Base(HarvesterBase):

    def glassLAIavhrr(self):

        url_base = 'ftp://ftp.glcf.umd.edu/glcf/GLASS/LAI/AVHRR/'

        years = ['1982', '1983', '1984', '1985', '1986', '1987', '1988',
                 '1989', '1990', '1991', '1992', '1993', '1994', '1995',
                 '1996', '1997', '1998', '1999', '2000', '2001', '2002',
                 '2003', '2004', '2005', '2006', '2007', '2008', '2009',
                 '2010', '2011', '2012', '2013', '2014', '2015']

        eight_days = ['001', '009', '017', '025', '033', '041', '049',
                      '057', '065', '073', '081', '089', '097', '105',
                      '113', '121', '129', '137', '145', '153', '161',
                      '169', '177', '185', '193', '201', '209', '217',
                      '225', '233', '241', '249', '257', '265', '273',
                      '281', '289', '297', '305', '313', '321', '329',
                      '337', '345', '353', '361']

        spatial_template = '{{"type":"Polygon", "coordinates":[{}]}}'
        spatial = spatial_template.format([[-180, 90], [180, 90], [180, -90], [-180, -90], [-180, 90]])  # noqa: E501
        title = 'GLASS Leaf Area Index AVHRR 8 days'
        description = 'The GLASS Leaf Area Index (LAI) product, a global LAI product with long time series, generated from AVHRR reflectance, and released by the Center for Global Change Data Processing and Analysis of Beijing Normal University. The GLASS LAI product has a temporal resolution of 8 days and is available from 1982 to 2015.'  # noqa: E501

        glass_lai_avhrr_products = []

        for i in years:
            for j in eight_days:
                start_date = datetime.datetime(int(i), 1, 1, 0, 0, 0) + datetime.timedelta(int(j) - 1)  # noqa: E501
                if (start_date == datetime.datetime(int(i), 12, 26, 0, 0, 0)) or (start_date == datetime.datetime(int(i), 12, 27, 0, 0, 0)):  # noqa: E501
                    end_date = datetime.datetime(int(i), 12, 31, 23, 59, 59)
                else:
                    end_date = datetime.datetime(int(i), 1, 1, 23, 59, 59) + datetime.timedelta(int(j) + 6)  # noqa: E501
                filename = 'GLASS01B02.V04.A' + i + j + '.2017269.hdf'
                filename_no_ext = filename.replace('.hdf', '')
                identifier = filename.replace('.hdf', '').replace('.', '_')
                file_url = url_base + i + '/' + filename
                thumbnail_url = url_base + i + '/' + filename_no_ext + '.LAI.jpg'  # noqa: E501
                metadata_url = url_base + i + '/' + filename + '.xml'
                glass_lai_avhrr_products.append([title, description, start_date, end_date, spatial, filename, identifier, file_url, thumbnail_url, metadata_url, [{'name': 'LAI'}, {'name': 'leaf area index'}, {'name': 'AVHRR'}, {'name': 'GLASS'}, {'name': 'global'}, {'name': 'GLCF'}]])  # noqa: E501

        return glass_lai_avhrr_products

    def glassLAImodis(self):

        url_base = 'ftp://ftp.glcf.umd.edu/glcf/GLASS/LAI/MODIS/0.05D/'

        years = ['2001', '2002', '2003', '2004', '2005', '2006', '2007',
                 '2008', '2009', '2010', '2011', '2012', '2013', '2014',
                 '2015']

        eight_days = ['001', '009', '017', '025', '033', '041', '049',
                      '057', '065', '073', '081', '089', '097', '105',
                      '113', '121', '129', '137', '145', '153', '161',
                      '169', '177', '185', '193', '201', '209', '217',
                      '225', '233', '241', '249', '257', '265', '273',
                      '281', '289', '297', '305', '313', '321', '329',
                      '337', '345', '353', '361']

        spatial_template = '{{"type":"Polygon", "coordinates":[{}]}}'
        spatial = spatial_template.format([[-180, 90], [180, 90], [180, -90], [-180, -90], [-180, 90]])  # noqa: E501
        title = 'GLASS Leaf Area Index MODIS 8 days'
        description = 'The GLASS Leaf Area Index (LAI) product, a global LAI product with long time series, derived from MODIS land surface reflectance (MOD09A1), and released by the Center for Global Change Data Processing and Analysis of Beijing Normal University. The GLASS LAI product has a temporal resolution of 8 days and is available from 1982 to 2015.'  # noqa: E501

        glass_lai_modis_products = []

        for i in years:
            for j in eight_days:
                start_date = datetime.datetime(int(i), 1, 1, 0, 0, 0) + datetime.timedelta(int(j) - 1)  # noqa: E501
                if (start_date == datetime.datetime(int(i), 12, 26, 0, 0, 0)) or (start_date == datetime.datetime(int(i), 12, 27, 0, 0, 0)):  # noqa: E501
                    end_date = datetime.datetime(int(i), 12, 31, 23, 59, 59)
                else:
                    end_date = datetime.datetime(int(i), 1, 1, 23, 59, 59) + datetime.timedelta(int(j) + 6)  # noqa: E501
                if i == '2015':
                    filename = 'GLASS01B01.V04.A' + i + j + '.2016343.hdf'
                    file_url = url_base + i + '/' + filename
                    thumbnail_url = url_base + i + '/' + filename.replace('.hdf', '.jpg')  # noqa: E501
                    identifier = filename.replace('.hdf', '').replace('.', '_')
                    metadata_url = url_base + i + '/' + filename.replace('.2016343.hdf', '.2016341.hdf.xml')  # noqa: E501
                else:
                    filename = 'GLASS01B01.V04.A' + i + j + '.2016236.hdf'
                    filename_no_ext = filename.replace('.hdf', '')
                    identifier = filename.replace('.hdf', '').replace('.', '_')
                    file_url = url_base + i + '/' + filename
                    thumbnail_url = url_base + i + '/' + filename_no_ext + '.jpg'  # noqa: E501
                    metadata_url = url_base + i + '/' + filename + '.xml'
                glass_lai_modis_products.append([title, description, start_date, end_date, spatial, filename, identifier, file_url, thumbnail_url, metadata_url, [{'name': 'LAI'}, {'name': 'leaf area index'}, {'name': 'MODIS'}, {'name': 'GLASS'}, {'name': 'global'}, {'name': 'GLCF'}]])  # noqa: E501

        return glass_lai_modis_products

    def _create_object(self, sensor, dataset_info):

        extras = [HOExtra(key='status',
                          value='new')]

        if sensor == 'avhrr':
            collectionID = 'LAI_1KM_AVHRR_8DAYS_GL'
        elif sensor == 'modis':
            collectionID = 'LAI_1KM_MODIS_8DAYS_GL'
        title = dataset_info[0]
        description = dataset_info[1]
        start_date = dataset_info[2]
        end_date = dataset_info[3]
        spatial = dataset_info[4]
        filename = dataset_info[5]
        identifier = dataset_info[6]
        downloadURL = dataset_info[7]
        thumbnailURL = dataset_info[8]
        metadataURL = dataset_info[9]
        tags = dataset_info[10]

        content = json.dumps({'collectionID': collectionID, 'title': title, 'description': description, 'start_date': start_date, 'end_date': end_date,  # noqa: E501
                                'identifier': identifier, 'downloadURL': downloadURL, 'thumbnailURL': thumbnailURL, 'metadataURL': metadataURL,  # noqa: E501
                                'spatial': spatial, 'filename': filename,
                                'tags': tags}, default=str)

        obj = HarvestObject(job=self.job, guid=unicode(uuid.uuid4()),
                            extras=extras, content=content)

        obj.save()

        return obj.id

    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        resources = []

        resources.append(self._make_resource(metadata['downloadLink'], 'Product Download (HDF)'))  # noqa: E501
        resources.append(self._make_resource(metadata['metadataLink'], 'Metadata Download (XML)'))  # noqa: E501
        resources.append(self._make_resource(metadata['thumbnailLink'], 'Thumbnail Download (JPG)'))  # noqa: E501

        return resources

    def _make_resource(self, url, name, size=None):
        """Return a resource dictionary."""
        resource_dict = {}
        resource_dict['name'] = name
        resource_dict['url'] = url
        if 'HDF' in name:
            resource_dict['format'] = 'hdf'
            resource_dict['mimetype'] = 'application/x-hdf'
            resource_dict['description'] = ('Download the HDF file.')
        elif 'XML' in name:
            resource_dict['format'] = 'xml'
            resource_dict['mimetype'] = 'application/xml'
            resource_dict['description'] = ('Download the metadata XML file.')
        elif 'JPG' in name:
            resource_dict['format'] = 'jpg'
            resource_dict['mimetype'] = 'image/jpeg'
            resource_dict['description'] = ('Download a JPG quicklook.')

        return resource_dict

    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        content = json.loads(content)

        metadata = {}

        metadata['collection_id'] = (content['collectionID'])
        metadata['downloadLink'] = content['downloadURL']
        metadata['thumbnailLink'] = content['thumbnailURL']
        metadata['metadataLink'] = content['metadataURL']

        # Add common metadata
        metadata['title'] = content['title']
        metadata['notes'] = (content['description'])
        metadata['spatial'] = content['spatial']
        metadata['identifier'] = content['identifier']
        metadata['name'] = metadata['identifier'].lower()
        metadata['StartTime'] = '{}.000Z'.format(content['start_date'])
        metadata['StopTime'] = '{}.999Z'.format(content['end_date'])
        metadata['tags'] = content['tags']

        return metadata
