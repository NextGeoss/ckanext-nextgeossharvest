# -*- coding: utf-8 -*-

import logging
import json

from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class NoaEpidemicsBaseHarvester(HarvesterBase):
    
    def _parse_content(self, entry):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """

        content = json.loads(entry)
        item = {}

        # Product Information
        item['name'] = content['filename']
        item['title'] = content['filename']
        item['identifier'] = content['filename']
        item['notes'] = """Environmental, meteorological and geomorphological data for the {} mosquito-trap with ID: {}. For more information please visit: http://beyond-eocenter.eu/index.php/web-services/eywa""".format(content['mosquito_type'].title(), content['station_id']) # noqa: E501

        # Timerange
        if 'dt_corrected' in content:
            item['timerange_start'] = content['dt_corrected'] + "T00:00:00.000Z"
            item['timerange_end'] = content['dt_corrected'] + "T00:00:00.000Z"

        else:
            item['timerange_start'] = content['dt_placement'] + "T00:00:00.000Z"
            item['timerange_end'] = content['dt_placement'] + "T00:00:00.000Z"

        # Information like NDVI, NDBI etc.
        item = self._parse_product_extra_information(item, content)

        # Geometry
        item['spatial'] = content['spatial']

        # Tags
        item['tags'] = [{"name": "noa_epidemics"},
                        {"name": "mosquito"},
                        {"name": content["mosquito_type"].lower()}]

        # Product resource only
        item['resource'] = [{
            'name': content['filename'],
            'description': "Access all the information regarding this dataset at the NOA Epidemics API.",
            'url': "http://epidemics.space.noa.gr/api_v2/{}/?station_id={}&dt_placement={}".format(content['mosquito_type'], content['station_id'], content['dt_placement']),
            'format': "JSON",
            'mimetype': "application/json"}]

        # Colection Information
        item['collection_id'] = 'EYWA_{}_PRODUCTS'.format(content['mosquito_type'].upper())
        item['collection_name'] = 'EYWA (NOA-BEYOND) {} Products'.format(content['mosquito_type'].title())  # noqa: E501
        item['collection_description'] = """A 10-years’ time-series of environmental, meteorological and geomorphological data for every mosquito-trap in five countries, regarding {} mosquitoes. Satellite indices created and opened by EYWA. For more information please visit: http://beyond-eocenter.eu/index.php/web-services/eywa""".format(content['mosquito_type'].title())  # noqa: E501

        return item


    def _get_resources(self, parsed_content):
        """Return a list of resource dicts."""
        return parsed_content['resource']

    def _parse_product_extra_information(self, item, content):

        # Normalized Differences
        item['Normalized Difference Vegetation Index (NDVI)'] = str(content['ndvi']) + " (ndvi)"
        item['Normalized Difference Water Index (NDWI)'] = str(content['ndwi']) + " (ndwi)"
        item['Normalized Difference Moisture Index (NDMI)'] = str(content['ndmi']) + " (ndmi)"
        item['Normalized Difference Built Index (NDBI)'] = str(content['ndbi']) + " (ndbi)"
        # ND Mean
        item['Normalized Difference Vegetation Index (NDVI) Mean'] = str(content['ndvi_mean']) + " (ndvi_mean)"
        item['Normalized Difference Water Index (NDWI) Mean'] = str(content['ndwi_mean']) + " (ndwi_mean)"
        item['Normalized Difference Moisture Index (NDMI) Mean'] = str(content['ndmi_mean']) + " (ndmi_mean)"
        item['Normalized Difference Built Index (NDBI) Mean'] = str(content['ndbi_mean']) + " (ndbi_mean)"
        # ND STD
        item['Normalized Difference Vegetation Index (NDVI) Standard Deviation'] = str(content['ndvi_std']) + " (ndvi_std)"
        item['Normalized Difference Water Index (NDWI) Standard Deviation'] = str(content['ndwi_std']) + " (ndwi_std)"
        item['Normalized Difference Moisture Index (NDMI) Standard Deviation'] = str(content['ndmi_std']) + " (ndmi_std)"
        item['Normalized Difference Built Index (NDBI) Standard Deviation'] = str(content['ndbi_std']) + " (ndbi_std)"

        # Rainfall
        item['Accumulated Rainfall (1 week) [mm]'] = str(content['acc_rainfall_1week']) + " (acc_rainfall_1week)"
        item['Accumulated Rainfall (2 weeks) [mm]'] = str(content['acc_rainfall_2week']) + " (acc_rainfall_2week)"
        item['Accumulated Rainfall (January) [mm]'] = str(content['acc_rainfall_jan']) + " (acc_rainfall_jan)"
        # LST
        item['Land Surface Temperature (Day Average) [*]'] = str(content['lst_day']) + " (lst_day)"
        item['Land Surface Temperature (Night Average) [*]'] = str(content['lst_night']) + " (lst_night)"
        item['Land Surface Temperature (January Mean) [*]'] = str(content['lst_jan_mean']) + " (lst_jan_mean)"
        item['Land Surface Temperature (February Mean) [*]'] = str(content['lst_feb_mean']) + " (lst_feb_mean)"
        item['Land Surface Temperature (March Mean) [*]'] = str(content['lst_mar_mean']) + " (lst_mar_mean)"
        item['Land Surface Temperature (April Mean) [*]'] = str(content['lst_apr_mean']) + " (lst_apr_mean)"
        item['Land Surface Temperature Information [*]'] = """To convert Modis Scale into Kelvin/Celsius: 
                                                        Value x 0.02 (scale of Modis) - 273.15 (From Kelvin to Celsius) 
                                                        e.g. value = 14680*0.02 = 293.6 | 293.6 - 273.15 = 20.45 º Celsius"""
        
        # Wind
        if "max_wind" in content:
            item['Max wind speed [m/s]'] = str(content['max_wind']) + " (max_wind)"
        if "min_wind" in content:
            item['Min wind speed [m/s]'] = str(content['min_wind']) + " (min_wind)"
        if "mean_wind" in content:
            item['Mean wind speed [m/s]'] = str(content['mean_wind']) + " (mean_wind)"

        # Other
        item['Breeding sites within 1km'] = str(content['sp_cnt_1km']) + " (sp_cnt_1km)"
        item['Waste water treatment facilities within 1km'] = str(content['biol_cnt_1km']) + " (biol_cnt_1km)"
        item['Breeding site length and water course within 1km'] = str(content['wc_l_1km']) + " (wc_l_1km)"
        item['Total area of temporarily inundated areas within 1km'] = str(content['pg_area_1km']) + " (pg_area_1km)"
        item['Total area of wetlands within 1km'] = str(content['fs_area_1km']) + " (fs_area_1km)"
        item['Distance from watercourses within 1km [m]'] = str(content['wc_mean_dist_1000']) + " (wc_mean_dist_1000)"
        item['Mean distance from coastline [m]'] = str(content['coast_mean_dist_1000']) + " (coast_mean_dist_1000)"
        item['Mean elevation [m]'] = str(content['dem_mean_1km']) + " (dem_mean_1km)"
        item['Mean slope [deg]'] = str(content['slope_mean_1km']) + " (slope_mean_1km)"
        item['Mean aspect [deg]'] = str(content['aspect_mean_1km']) + " (aspect_mean_1km)"
        item['Distance from nearest water surface [m]'] = str(content['waw_mean_1km']) + " (waw_mean_1km)"
        item['Mean flow accumulation'] = str(content['flow_accu_1km']) + " (flow_accu_1km)"


        return item
