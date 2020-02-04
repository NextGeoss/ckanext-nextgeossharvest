# -*- coding: utf-8 -*-

import json
import uuid
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.model import HarvestObject


class EBVSBase(HarvesterBase):

    def treeSpecies(self):

        url_base = 'http://forest.jrc.ec.europa.eu/media/efdac_species/'

        tree_species = {'Acer campestre': 'sp1', 'Acer pseudoplatanus': 'sp5', 'Alnus glutinosa': 'sp7', 'Betula pendula': 'sp10', 'Betula pubescens': 'sp11', 'Carpinus betulus': 'sp13', 'Castanea sativa': 'sp15', 'Eucalyptus sp.': 'sp17', 'Fagus sylvatica': 'sp20', 'Fraxinus excelsior': 'sp22', 'Ostrya carpinifolia': 'sp29', 'Populus tremula': 'sp35', 'Prunus avium': 'sp36', 'Quercus cerris': 'sp41', 'Quercus frainetto': 'sp44', 'Quercus ilex': 'sp46', 'Quercus petraea': 'sp48', 'Quercus pubescens': 'sp49', 'Quercus robur': 'sp51', 'Quercus suber': 'sp54', 'Robinia pseudacacia': 'sp56', 'Tilia cordata': 'sp68', 'Abies alba': 'sp100', 'Larix decidua': 'sp116'}  # noqa: E501

        spatial_template = '{{"type":"Polygon", "coordinates":[{}]}}'
        spatial = spatial_template.format([[-31.949603, 67.342906], [51.229700, 67.538050], [27.855047, 34.096753], [-8.254897, 34.016522], [-31.949603, 67.342906]])  # noqa: E501

        baseline_products = []
        future_products = []

        for i in tree_species:

            title_baseline = 'Tree Species Distribution - ' + i + ' Habitat Suitability Baseline'  # noqa: E501
            title_future = 'Tree Species Distribution - ' + i + ' Habitat Suitability Future'  # noqa: E501
            description_baseline = 'European Distribution of the specie ' + i + ' for the year 2000 (Habitat Suitability baseline).'  # noqa: E501
            description_future = 'European Distribution of the specie ' + i + ' for the years 2020, 2050 and 2080, based on different models such as ENS, CCCMA, CSIRO, HADCM3 (Habitat Suitability future).'  # noqa: E501
            filename_baseline = tree_species[i] + '_suit_baseline.tar.gz'
            filename_future = tree_species[i] + '_suit_future.tar.gz'
            baseline_id = filename_baseline.replace('.tar.gz', '')
            future_id = filename_future.replace('.tar.gz', '')
            baseline_url = url_base + filename_baseline
            future_url = url_base + filename_future

            baseline_products.append([title_baseline, description_baseline, '2000-01-01', '2000-12-31', spatial, filename_baseline, baseline_id, baseline_url, [{'name': 'europe'}, {'name': 'tree'}, {'name': 'tree species'}, {'name': 'tree species distribution'}, {'name': 'species'}, {'name': 'habitat suitability'}, {'name': 'habitat'}, {'name': 'suitability'}, {'name': 'biodiversity'}, {'name': 'EBV'}, {'name': 'JRC'}, {'name': i}, {'name': '2000'}, {'name': 'baseline'}]])  # noqa: E501
            future_products.append([title_future, description_future, '2020-01-01', '2080-12-31', spatial, filename_future, future_id, future_url, [{'name': 'europe'}, {'name': 'tree'}, {'name': 'tree species'}, {'name': 'tree species distribution'}, {'name': 'species'}, {'name': 'habitat suitability'}, {'name': 'habitat'}, {'name': 'suitability'}, {'name': 'biodiversity'}, {'name': 'EBV'}, {'name': 'JRC'}, {'name': i}, {'name': '2020'}, {'name': '2050'}, {'name': '2080'}, {'name': 'future'}, {'name': 'ENS'}, {'name': 'CCCMA'}, {'name': 'CSIRO'}, {'name': 'HADCM3'}]])  # noqa: E501

        tree_species_products = baseline_products + future_products
        return tree_species_products

    def floodHazards(self):

        url_base = 'http://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/FLOODS/'

        flood_hazards = {'Europe100': '100', 'Europe10': '10', 'Europe20': '20', 'Europe200': '200', 'Europe50': '50', 'Europe500': '500', 'World10': '10', 'World100': '100', 'World20': '20', 'World200': '200', 'World50': '50', 'World500': '500', 'permanent': 'Map of permanent water bodies of the World'}  # noqa: E501

        spatial_template = '{{"type":"Polygon", "coordinates":[{}]}}'
        spatial_eu = spatial_template.format([[-31.949603, 67.342906], [51.229700, 67.538050], [27.855047, 34.096753], [-8.254897, 34.016522], [-31.949603, 67.342906]])  # noqa: E501
        spatial_gl = spatial_template.format([[-180, 90], [180, 90], [180, -90], [-180, -90], [-180, 90]])  # noqa: E501

        flood_products = []

        for i in flood_hazards:
            if i == 'permanent':
                title = flood_hazards[i]
                description = 'The map represents permanent water bodies at global scale (lakes and reservoirs), derived from a corrected version of the Global Lakes and Wetlands Database. Resolution is 30 arcseconds (approx. 1km). Natural water bodies (lakes) are indicated by value 1, Reservoirs are indicated by value 2. NOTE: this dataset should be used to integrate the JRC global flood hazard maps. It is not an official flood hazard map.'  # noqa: E501
                filename = 'floodMapGL_permWB.zip'
                identifier = filename.replace('.zip', '')
                url = url_base + 'GlobalMaps/' + filename
                spatial = spatial_gl
                date = '2016-11-04'
                tags = [{'name': 'flood hazard'}, {'name': 'global'}, {'name': 'map'}, {'name': 'flood'}, {'name': 'biodiversity'}, {'name': 'EBV'}, {'name': 'JRC'}]  # noqa: E501
            elif 'Europe' in i:
                title = 'Flood hazard map for Europe - ' + flood_hazards[i] + '-year return period'  # noqa: E501
                description = 'The map depicts flood prone areas in Europe for flood events with ' + flood_hazards[i] + '-year return period. Cell values indicate water depth (in m). The map can be used to assess flood exposure and risk of population and assets. NOTE: this dataset is based on JRC elaborations and is not an official flood hazard map.'  # noqa: E501
                filename = 'floodMapEU_rp' + flood_hazards[i] + 'y.zip'
                identifier = filename.replace('.zip', '')
                url = url_base + 'EuropeanMaps/' + filename
                spatial = spatial_eu
                date = '2016-11-02'
                tags = [{'name': 'Europe'}, {'name': 'flood hazard'}, {'name': 'map'}, {'name': 'flood'}, {'name': 'biodiversity'}, {'name': 'EBV'}, {'name': 'JRC'}]  # noqa: E501
            elif 'World' in i:
                title = 'Flood hazard map of the World - ' + flood_hazards[i] + '-year return period'  # noqa: E501
                description = 'The map depicts flood prone areas at global scale for flood events with ' + flood_hazards[i] + '-year return period. Resolution is 30 arcseconds (approx. 1km). Cell values indicate water depth (in m). The map can be used to assess flood exposure and risk of population and assets. NOTE: this dataset is based on JRC elaborations and is not an official flood hazard map.'  # noqa: E501
                filename = 'floodMapGL_rp' + flood_hazards[i] + 'y.zip'
                identifier = filename.replace('.zip', '')
                url = url_base + 'GlobalMaps/' + filename
                spatial = spatial_gl
                date = '2016-11-02'
                tags = [{'name': 'flood hazard'}, {'name': 'global'}, {'name': 'map'}, {'name': 'flood'}, {'name': 'biodiversity'}, {'name': 'EBV'}, {'name': 'JRC'}]  # noqa: E501

            flood_products.append([title, description, date, date, spatial, filename, identifier, url, tags])  # noqa: E501
        return flood_products

    def _create_object(self, ebv_type, dataset_info):

        extras = [HOExtra(key='status',
                          value='new')]

        if ebv_type == 'tree_species':
            collectionID = 'TREE_SPECIES_DISTRIBUTION_HABITAT_SUITABILITY'
        elif ebv_type == 'flood_hazards':
            collectionID = 'FLOOD_HAZARD_EU_GL'
        title = dataset_info[0]
        description = dataset_info[1]
        start_date = dataset_info[2]
        end_date = dataset_info[3]
        spatial = dataset_info[4]
        filename = dataset_info[5]
        identifier = dataset_info[6]
        download_url = dataset_info[7]
        tags = dataset_info[8]

        content = json.dumps({'collectionID': collectionID, 'title': title, 'description': description, 'start_date': start_date, 'end_date': end_date,  # noqa: E501
                                'identifier': identifier, 'downloadURL': download_url,  # noqa: E501
                                'spatial': spatial, 'filename': filename,
                                'tags': tags}, default=str)

        obj = HarvestObject(job=self.job, guid=unicode(uuid.uuid4()),
                            extras=extras, content=content)

        obj.save()

        return obj.id

    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        return metadata['resource']

    def _make_resource(self, url, name):
        """Return a resource dictionary."""
        resource_dict = {}
        resource_dict['name'] = name
        resource_dict['url'] = url

        if 'tar.gz' in resource_dict['url']:
            resource_dict['format'] = 'gtar'
            resource_dict['mimetype'] = 'application/x-tar-gzip'
        else:
            resource_dict['format'] = 'zip'
            resource_dict['mimetype'] = 'application/zip'

        return resource_dict

    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        content = json.loads(content)

        metadata = {}

        metadata['collection_id'] = (content['collectionID'])
        metadata['resource'] = [self._make_resource(content['downloadURL'],
                                                    'Product Download')]

        # Add common metadata
        metadata['title'] = content['title']
        metadata['notes'] = (content['description'])
        metadata['spatial'] = content['spatial']
        metadata['identifier'] = content['identifier']
        metadata['name'] = metadata['identifier'].lower()
        metadata['timerange_start'] = '{}T00:00:00.000Z'.format(content['start_date'])  # noqa E501
        metadata['timerange_end'] = '{}T23:59:59.999Z'.format(content['end_date'])  # noqa E501
        metadata['tags'] = content['tags']

        return metadata
