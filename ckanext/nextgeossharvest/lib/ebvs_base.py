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

    def phenology_avhrr(self):

        url_base = 'https://dds.cr.usgs.gov/ltaauth/hsm/lta4/archive/phenology/avhrr/'  # noqa: E501
        spatial_template = '{{"type":"Polygon", "coordinates":[{}]}}'
        spatial = spatial_template.format([[-65.3946499, 46.7049], [-128.5300582, 48.4030555], [-119.9722888, 23.5837583], [-75.4163527, 22.4793916], [-65.3946499, 46.7049]])  # noqa: E501

        avhrr_data = {'1989': ['PHAVHRR1989V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1989V01&did=463736604', 'PHAVHRR1989V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1989V01&did=463736401'],  # noqa: E501
            '1990': ['PHAVHRR1990V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1990V01&did=463737323', 'PHAVHRR1990V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1990V01&did=463737204'],  # noqa: E501
            '1991': ['PHAVHRR1991V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1991V01&did=463737635', 'PHAVHRR1991V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1991V01&did=463737728'],  # noqa: E501
            '1992': ['PHAVHRR1992V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1992V01&did=463737874', 'PHAVHRR1992V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1992V01&did=463737999'],  # noqa: E501
            '1993': ['PHAVHRR1993V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1993V01&did=463738171', 'PHAVHRR1993V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1993V01&did=463738294'],  # noqa: E501
            '1994': ['PHAVHRR1994V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1994V01&did=463738431', 'PHAVHRR1994V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1994V01&did=463738560'],  # noqa: E501
            '1995': ['PHAVHRR1995V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1995V01&did=463738785', 'PHAVHRR1995V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1995V01&did=463738884'],  # noqa: E501
            '1996': ['PHAVHRR1996V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1996V01&did=463739698', 'PHAVHRR1996V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1996V01&did=463738991'],  # noqa: E501
            '1997': ['PHAVHRR1997V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1997V01&did=463740012', 'PHAVHRR1997V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1997V01&did=463740092'],  # noqa: E501
            '1998': ['PHAVHRR1998V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1998V01&did=463740389', 'PHAVHRR1998V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1998V01&did=463740266'],  # noqa: E501
            '1999': ['PHAVHRR1999V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1999V01&did=463740981', 'PHAVHRR1999V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR1999V01&did=463740830'],  # noqa: E501
            '2000': ['PHAVHRR2000V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2000V01&did=463741279', 'PHAVHRR2000V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2000V01&did=463741375'],  # noqa: E501
            '2001': ['PHAVHRR2001V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2001V01&did=463741768', 'PHAVHRR2001V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2001V01&did=463741845'],  # noqa: E501
            '2002': ['PHAVHRR2002V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2002V01&did=463743893', 'PHAVHRR2002V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2002V01&did=463743789'],  # noqa: E501
            '2003': ['PHAVHRR2003V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2003V01&did=463744409', 'PHAVHRR2003V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2003V01&did=463744276'],  # noqa: E501
            '2004': ['PHAVHRR2004V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2004V01&did=463744712', 'PHAVHRR2004V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2004V01&did=463744800'],  # noqa: E501
            '2005': ['PHAVHRR2005V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2005V01&did=463745271', 'PHAVHRR2005V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2005V01&did=463745491'],  # noqa: E501
            '2006': ['PHAVHRR2006V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHAVHRR2006V01&did=464196905', 'PHAVHRR2006V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2006V01&did=463745986'],  # noqa: E501
            '2007': ['PHAVHRR2007V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2007V01&did=463747455', 'PHAVHRR2007V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHAVHRR2007V01&did=464197046'],  # noqa: E501
            '2008': ['PHAVHRR2008V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2008V01&did=463749170', 'PHAVHRR2008V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHAVHRR2009V01&did=464197195'],  # noqa: E501
            '2009': ['PHAVHRR2009V01_TIF.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2009V01&did=463749823', 'PHAVHRR2009V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2009V01&did=463750044'],  # noqa: E501
            '2010': ['PHAVHRR2010V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHAVHRR2010V01&did=464197489', 'PHAVHRR2010V01_ENVI.ZIP?id=b5lahcvbcs9i6sduuo825kna22&iid=PHAVHRR2010V01&did=463751654'],  # noqa: E501
            '2011': ['PHAVHRR2011V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHAVHRR2011V01&did=464197655', 'PHAVHRR2011V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHAVHRR2011V01&did=464197738'],  # noqa: E501
            '2012': ['PHAVHRR2012V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHAVHRR2012V01&did=464197830', 'PHAVHRR2012V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHAVHRR2012V01&did=464197899'],  # noqa: E501
            '2013': ['PHAVHRR2013V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHAVHRR2013V01&did=464210921', 'PHAVHRR2013V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHAVHRR2013V01&did=464210983']}  # noqa: E501

        avhrr_products = []

        for i in avhrr_data:

            identifier = 'PHAVHRR' + i + 'V01'
            filename_tif = identifier + '_TIF.ZIP'
            filename_envi = identifier + '_ENVI.ZIP'
            url_tif = url_base + avhrr_data[i][0] + '&ver=production'
            url_envi = url_base + avhrr_data[i][1] + '&ver=production'
            start_date = i + '-01-01'
            end_date = i + '-12-31'
            tags = [{'name': 'phenology'}, {'name': 'avhrr'}, {'name': 'USA'}, {'name': 'NOAA'}, {'name': 'annual'}, {'name': '1KM'}, {'name': 'biodiversity'}, {'name': 'EBV'}]  # noqa: E501

            avhrr_products.append([identifier, filename_tif, filename_envi, url_tif, url_envi, start_date, end_date, spatial, tags])  # noqa: E501
        return avhrr_products

    def phenology_emodis(self):

        url_base = 'https://dds.cr.usgs.gov/ltaauth/hsm/lta4/archive/phenology/emodis/'  # noqa: E501
        spatial_template = '{{"type":"Polygon", "coordinates":[{}]}}'
        spatial_se = spatial_template.format([[-65.3946499, 46.7049], [-96.4768193, 51.7223332], [-97.6095277, 25.664875], [-75.4163527, 22.4793916], [-65.3946499, 46.7049]])  # noqa: E501
        spatial_sw = spatial_template.format([[-96.4768193, 51.7223332], [-128.5300582, 48.4030555], [-119.9722888, 23.5837583], [-97.6095277, 25.664875], [-96.4768193, 51.7223332]])  # noqa: E501

        emodis_data = {'2001SE': ['PHEMUSE2001V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2001V01&did=464184573', 'PHEMUSE2001V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2001V01&did=464184736'],  # noqa: E501
            '2001SW': ['PHEMUSW2001V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2001V01&did=464184916', 'PHEMUSW2001V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2001V01&did=464185087'],  # noqa: E501
            '2002SE': ['PHEMUSE2002V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2002V01&did=464184370', 'PHEMUSE2002V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2002V01&did=464184293'],  # noqa: E501
            '2002SW': ['PHEMUSW2002V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2002V01&did=464185334', 'PHEMUSW2002V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2002V01&did=464185430'],  # noqa: E501
            '2003SE': ['PHEMUSE2003V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2003V01&did=464185742', 'PHEMUSE2003V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2003V01&did=464185900'],  # noqa: E501
            '2003SW': ['PHEMUSW2003V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2003V01&did=464186120', 'PHEMUSW2003V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2003V01&did=464186165'],  # noqa: E501
            '2004SE': ['PHEMUSE2004V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2004V01&did=464186236', 'PHEMUSE2004V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2004V01&did=464186360'],  # noqa: E501
            '2004SW': ['PHEMUSW2004V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2004V01&did=464186432', 'PHEMUSW2004V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2004V01&did=464186541'],  # noqa: E501
            '2005SE': ['PHEMUSE2005V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2005V01&did=464186645', 'PHEMUSE2005V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2005V01&did=464186711'],  # noqa: E501
            '2005SW': ['PHEMUSW2005V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2005V01&did=464186866', 'PHEMUSW2005V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2005V01&did=464186901'],  # noqa: E501
            '2006SE': ['PHEMUSE2006V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2006V01&did=464187095', 'PHEMUSE2006V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2006V01&did=464187245'],  # noqa: E501
            '2006SW': ['PHEMUSW2006V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2006V01&did=464187385', 'PHEMUSW2006V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2006V01&did=464187446'],  # noqa: E501
            '2007SE': ['PHEMUSE2007V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2007V01&did=464187596', 'PHEMUSE2007V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2007V01&did=464187687'],  # noqa: E501
            '2007SW': ['PHEMUSW2007V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2007V01&did=464188232', 'PHEMUSW2007V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2007V01&did=464188071'],  # noqa: E501
            '2008SE': ['PHEMUSE2008V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2008V01&did=464188650', 'PHEMUSE2008V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2008V01&did=464188509'],  # noqa: E501
            '2008SW': ['PHEMUSW2008V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2008V01&did=464188905', 'PHEMUSW2008V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2008V01&did=464189009'],  # noqa: E501
            '2009SE': ['PHEMUSE2009V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2009V01&did=464189152', 'PHEMUSE2009V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2009V01&did=464189234'],  # noqa: E501
            '2009SW': ['PHEMUSW2009V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2009V01&did=464189768', 'PHEMUSW2009V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2009V01&did=464189817'],  # noqa: E501
            '2010SE': ['PHEMUSE2010V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2010V01&did=464189889', 'PHEMUSE2010V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2010V01&did=464189945'],  # noqa: E501
            '2010SW': ['PHEMUSE2010V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2010V01&did=464189889', 'PHEMUSW2010V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2010V01&did=464190135'],  # noqa: E501
            '2011SE': ['PHEMUSE2011V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2011V01&did=464190222', 'PHEMUSE2011V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2011V01&did=464190361'],  # noqa: E501
            '2011SW': ['PHEMUSW2011V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2011V01&did=464190452', 'PHEMUSW2011V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2011V01&did=464190545'],  # noqa: E501
            '2012SE': ['PHEMUSE2012V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2012V01&did=464190805', 'PHEMUSE2012V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2012V01&did=464190847'],  # noqa: E501
            '2012SW': ['PHEMUSW2012V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2012V01&did=464191016', 'PHEMUSW2012V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2012V01&did=464191184'],  # noqa: E501
            '2013SE': ['PHEMUSE2013V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2013V01&did=464191279', 'PHEMUSE2013V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2013V01&did=464191331'],  # noqa: E501
            '2013SW': ['PHEMUSW2013V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2013V01&did=464191438', 'PHEMUSW2013V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2013V01&did=464191538'],  # noqa: E501
            '2014SE': ['PHEMUSE2014V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2014V01&did=464191904', 'PHEMUSE2014V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2014V01&did=464191949'],  # noqa: E501
            '2014SW': ['PHEMUSW2014V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2014V01&did=464192025', 'PHEMUSW2014V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2014V01&did=464192071'],  # noqa: E501
            '2015SE': ['PHEMUSE2015V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2015V01&did=464192376', 'PHEMUSE2015V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2015V01&did=464192437'],  # noqa: E501
            '2015SW': ['PHEMUSW2015V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2015V01&did=464193109', 'PHEMUSW2015V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2015V01&did=464192997'],  # noqa: E501
            '2016SE': ['PHEMUSE2016V02_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2016V02&did=464192628', 'PHEMUSE2016V02_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2016V02&did=464192734'],  # noqa: E501
            '2016SW': ['PHEMUSW2016V02_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2016V02&did=464192793', 'PHEMUSW2016V02_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2016V02&did=464192847'],  # noqa: E501
            '2017SE': ['PHEMUSE2017V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2017V01&did=464192215', 'PHEMUSE2017V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSE2017V01&did=464192255'],  # noqa: E501
            '2017SW': ['PHEMUSW2017V01_TIF.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2017V01&did=464193189', 'PHEMUSW2017V01_ENVI.ZIP?id=25u8neuki6kbrjrv3763ncb4e5&iid=PHEMUSW2017V01&did=464193229']}  # noqa: E501

        emodis_products = []

        for i in emodis_data:

            if 'SE' in i:
                coord = 'SE'
                year = i.replace(coord, '')
                identifier = 'PHEMUSE' + year + 'V01'
                tags = [{'name': 'phenology'}, {'name': 'emodis'}, {'name': 'USA'}, {'name': 'NOAA'}, {'name': 'annual'}, {'name': '250M'}, {'name': 'SE'}, {'name': 'biodiversity'}, {'name': 'EBV'}]  # noqa: E501
                spatial = spatial_se
            elif 'SW' in i:
                coord = 'SW'
                year = i.replace(coord, '')
                identifier = 'PHEMUSW' + year + 'V01'
                tags = [{'name': 'phenology'}, {'name': 'emodis'}, {'name': 'USA'}, {'name': 'NOAA'}, {'name': 'annual'}, {'name': '250M'}, {'name': 'SW'}, {'name': 'biodiversity'}, {'name': 'EBV'}]  # noqa: E501
                spatial = spatial_sw

            filename_tif = identifier + '_TIF.ZIP'
            filename_envi = identifier + '_ENVI.ZIP'
            url_tif = url_base + emodis_data[i][0] + '&ver=production'
            url_envi = url_base + emodis_data[i][1] + '&ver=production'
            start_date = year + '-01-01'
            end_date = year + '-12-31'

            emodis_products.append([identifier, filename_tif, filename_envi, url_tif, url_envi, start_date, end_date, spatial, tags])  # noqa: E501
        return emodis_products

    def _create_object(self, ebv_type, dataset_info):

        extras = [HOExtra(key='status',
                          value='new')]

        if ebv_type == 'tree_species' or ebv_type == 'flood_hazards':
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
            downloadURL = dataset_info[7]
            tags = dataset_info[8]

            content = json.dumps({'collectionID': collectionID, 'title': title, 'description': description, 'start_date': start_date, 'end_date': end_date,  # noqa: E501
                                  'identifier': identifier, 'downloadURL': downloadURL,  # noqa: E501
                                  'spatial': spatial, 'filename': filename,
                                  'tags': tags}, default=str)

        elif ebv_type == 'phen_avhrr' or ebv_type == 'phen_emodis':
            if ebv_type == 'phen_avhrr':
                collectionID = 'RSP_AVHRR_1KM_ANNUAL_USA'
                title = 'AVHRR Remote Sensing Phenology - Annual'
                description = 'The Remote Sensing Phenology (RSP) collection is a set of nine annual phenological metrics for the conterminous United States. Researchers at the U.S. Geological Survey (USGS) Earth Resources Observation and Science (EROS) Center utilize data gathered by satellite sensors to track seasonal changes in vegetation. These datasets are provided by the sensor Advanced Very High Resolution Radiometer (AVHRR) from National Oceanic and Atmospheric Administration (NOAA) polar-orbiting satellites.'  # noqa: E501
            elif ebv_type == 'phen_emodis':
                collectionID = 'EMODIS_PHENOLOGY_250M_ANNUAL_USA'
                title = 'eMODIS Remote Sensing Phenology - Annual'
                description = 'The Remote Sensing Phenology (RSP) collection is a set of nine annual phenological metrics for the conterminous United States. Researchers at the U.S. Geological Survey (USGS) Earth Resources Observation and Science (EROS) Center utilize data gathered by satellite sensors to track seasonal changes in vegetation. These datasets are provided by the sensor Moderate Resolution Imaging Spectroradiometer (MODIS) carried aboard National Aeronautics and Space Administration (NASA) Terra and Aqua satellites.'  # noqa: E501
            identifier = dataset_info[0]
            filename_tif = dataset_info[1]
            filename_envi = dataset_info[2]
            downloadURL_tif = dataset_info[3]
            downloadURL_envi = dataset_info[4]
            start_date = dataset_info[5]
            end_date = dataset_info[6]
            spatial = dataset_info[7]
            tags = dataset_info[8]

            content = json.dumps({'collectionID': collectionID, 'title': title, 'description': description, 'start_date': start_date, 'end_date': end_date,  # noqa: E501
                                  'identifier': identifier, 'downloadURL_tif': downloadURL_tif,  # noqa: E501
                                  'downloadURL_envi': downloadURL_envi, 'spatial': spatial, 'filename_tif': filename_tif,  # noqa: E501
                                   'filename_envi': filename_envi, 'tags': tags}, default=str)  # noqa: E501

        obj = HarvestObject(job=self.job, guid=unicode(uuid.uuid4()),
                            extras=extras, content=content)

        obj.save()

        return obj.id

    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        resources = []

        if 'downloadLink' in metadata:
            resources.append(self._make_resource(metadata['downloadLink'],
                                                 'Product Download'))
        else:
            resources.append(self._make_resource(metadata['downloadLinkTIF'],
                                                 'Product Download (TIF)'))  # noqa: E501
            resources.append(self._make_resource(metadata['downloadLinkENVI'],
                                                 'Product Download (ENVI)'))  # noqa: E501

        return resources

    def _make_resource(self, url, name, size=None):
        """Return a resource dictionary."""
        resource_dict = {}
        resource_dict['name'] = name
        resource_dict['url'] = url
        if 'TIF' in name:
            resource_dict['format'] = 'tiff'
            resource_dict['mimetype'] = 'image/tiff'
            resource_dict['description'] = ('Download the TIF'
                                            ' product in a ZIP file.')
        elif 'ENVI' in name:
            resource_dict['format'] = 'envi'
            resource_dict['mimetype'] = 'application/octet-stream'
            resource_dict['description'] = ('Download the ENVI'
                                            ' product in a ZIP file.')
        else:
            if 'tar.gz' in resource_dict['url']:
                resource_dict['format'] = 'gtar'
                resource_dict['mimetype'] = 'application/x-tar-gzip'
            else:
                resource_dict['format'] = 'zip'
                resource_dict['mimetype'] = 'application/zip'
        if size:
            resource_dict['size'] = size

        return resource_dict

    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        content = json.loads(content)

        metadata = {}

        if content['collectionID'] == 'TREE_SPECIES_DISTRIBUTION_HABITAT_SUITABILITY':  # noqa: E501
            metadata['collection_id'] = (content['collectionID'])
            metadata['downloadLink'] = content['downloadURL']

        elif content['collectionID'] == 'FLOOD_HAZARD_EU_GL':
            metadata['collection_id'] = (content['collectionID'])
            metadata['downloadLink'] = content['downloadURL']

        elif content['collectionID'] == 'RSP_AVHRR_1KM_ANNUAL_USA':
            metadata['collection_id'] = (content['collectionID'])
            metadata['downloadLinkTIF'] = content['downloadURL_tif']
            metadata['downloadLinkENVI'] = content['downloadURL_envi']

        elif content['collectionID'] == 'EMODIS_PHENOLOGY_250M_ANNUAL_USA':
            metadata['collection_id'] = (content['collectionID'])
            metadata['downloadLinkTIF'] = content['downloadURL_tif']
            metadata['downloadLinkENVI'] = content['downloadURL_envi']

        # Add common metadata
        metadata['title'] = content['title']
        metadata['notes'] = (content['description'])
        metadata['spatial'] = content['spatial']
        metadata['identifier'] = content['identifier']
        metadata['name'] = metadata['identifier'].lower()
        metadata['StartTime'] = '{}T00:00:00.000Z'.format(content['start_date'])  # noqa E501
        metadata['StopTime'] = '{}T23:59:59.999Z'.format(content['end_date'])  # noqa E501
        metadata['timerange_start'] = metadata['StartTime']
        metadata['timerange_end'] = metadata['StopTime']
        metadata['tags'] = content['tags']

        return metadata
