# -*- coding: utf-8 -*-

import logging
import json
from datetime import timedelta, datetime
import uuid
from ftplib import FTP, error_perm as Ftp5xxErrors
from os import path

from dateutil.relativedelta import relativedelta

from ckan.model import Session
from ckan.model import Package

from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.model import HarvestObject


log = logging.getLogger(__name__)


class CMEMSBase(HarvesterBase):

    def _was_harvested(self, identifier, update_flag):
        """
        Check if a product has already been harvested and return True or False.
        """

        package = Session.query(Package) \
            .filter(Package.name == identifier.lower()).first()

        if package:
            if update_flag:
                log.debug('{} already exists and will be updated.'.format(identifier))  # noqa: E501
                status = 'change'
            else:
                log.debug('{} will not be updated.'.format(identifier))
                status = 'unchanged'
        else:
            log.debug('{} has not been harvested before. Attempting to harvest it.'.format(identifier))  # noqa: E501
            status = 'new'

        return status, package

    def _make_stop_time(self, start_date):
        stop_date = start_date + timedelta(days=1)
        return '{}T00:00:00.000Z'.format(stop_date.date())

    def _format_date_separed(self, date):
        day = datetime.strftime(date, '%d')
        month = datetime.strftime(date, '%m')
        year = datetime.strftime(date, '%Y')

        return day, month, year

    def _date_from_identifier_slv(self, identifier):
        identifier_parts = identifier.split('_')
        date_str = identifier_parts[5]
        date = datetime.strptime(date_str, '%Y%m%d')
        return date.strftime('%Y-%m-%d')

    def _date_from_identifier_gpaf(self, identifier):
        identifier_parts = identifier.split('_')
        date_str_start = identifier_parts[3]
        date_str_end = identifier_parts[4]
        date_start = datetime.strptime(date_str_start, '%Y%m%d')
        date_end = datetime.strptime(date_str_end[1:], '%Y%m%d')
        return date_start.strftime('%Y-%m-%d'), date_end.strftime('%Y-%m-%d')

    def _date_from_identifier_mog(self, identifier):
        identifier_parts = identifier.split('_')
        date_str = identifier_parts[1]
        date = datetime.strptime(date_str, '%Y%m%dT%fZ')
        return date.strftime('%Y-%m-%d')
        
    def _bulletin_date_medeos(self, identifier):
    	identifier_parts = identifier.split('-')
    	date_str = identifier_parts[6].replace('b','').replace('_re','')
    	date = datetime.strptime(date_str, '%Y%m%d')
    	return date.strftime('%Y-%m-%d')

    def _create_tags(self):
        """Create a list of tags based on the type of harvester."""
        tags_list = [{"name": "CMEMS"}]

        if self.harvester_type == 'sst':
            tags_list.extend([{"name": "SST"},
                              {"name": "sea surface temperature"},
                              {"name": "sea ice area fraction"},
                              {"name": "SIC"},
                              {"name": "temperature"},
                              {"name": "sea"},
                              {"name": "observation"}])
        elif self.harvester_type == 'ocn':
            tags_list.extend([{"name": "arctic"},
                              {"name": "arctic ocean"},
                              {"name": "north"},
                              {"name": "northern"},
                              {"name": "forecast"},
                              {"name": "temperature"},
                              {"name": "salinity"},
                              {"name": "sea surface height"},
                              {"name": "SSH"},
                              {"name": "sea ice fraction"},
                              {"name": "sea ice thickness"},
                              {"name": "surface snow thickness"},
                              {"name": "sea"},
                              {"name": "ice"},
                              {"name": "sea ice velocity"}])
        elif self.harvester_type == 'slv':
            tags_list.extend([{"name": "sea level"},
                              {"name": "sea level anomaly"},
                              {"name": "geostrophic"},
                              {"name": "velocity"},
                              {"name": "sea"},
                              {"name": "currents"},
                              {"name": "geostrophic velocity"}])
        elif self.harvester_type == 'gpaf':
            tags_list.extend([{"name": "sea"},
                              {"name": "hourly"},
                              {"name": "currents"},
                              {"name": "velocity"},
                              {"name": "eastward velocity"},
                              {"name": "northward velocity"},
                              {"name": "sea water temperature"},
                              {"name": "temperature"},
                              {"name": "sea surface height"},
                              {"name": "forecast"}])
        elif self.harvester_type == 'mog':
            tags_list.extend([{"name": "sea"},
                              {"name": "velocity"},
                              {"name": "hourly"},
                              {"name": "eastward sea water velocity "},
                              {"name": "northward sea water velocity"}])
        
        elif self.harvester_type == 'med_phy':
            tags_list.extend([{"name": "salinity"},
                              {"name": "sea level"},
                              {"name": "currents"},
                              {"name": "depth"},
                              {"name": "mediterranean"},
                              {"name": "temperature"},
                              {"name": "sea"},
                              {"name": "observation"}])
                              
        elif self.harvester_type == 'med_bio':
            tags_list.extend([{"name": "co2"},
            		       {"name": "bio"},
            		       {"name": "carbon dioxide"},
            		       {"name": "nitrate"},
            		       {"name": "phosphate"},
                              {"name": "Biogeochemistry"},
                              {"name": "mediterranean"},
                              {"name": "sea"},
                              {"name": "observation"}])
        
        elif self.harvester_type == 'bs_sst_006':
            tags_list.extend([{"name": "SST"},
                              {"name": "sea surface temperature"},
                              {"name": "Black Sea"},
                              {"name": "temperature"},
                              {"name": "sea"},
                              {"name": "observation"}])
        
        elif self.harvester_type == 'bs_chl':
            tags_list.extend([{"name": "chlorophyll"},
                              {"name": "Black Sea"},
                              {"name": "sea"},
                              {"name": "Observation"},
                              {"name": "Sentinel-3 "}])
        
        elif self.harvester_type == 'bs_opt':
            tags_list.extend([{"name": "Black Sea"},
                              {"name": "Remote sensing"},
                              {"name": "sentinel-3"},
                              {"name": "observation"},
                              {"name": "sea"}])
        
        elif self.harvester_type == 'bs_phy_l4':
            tags_list.extend([{"name": "sea level"},
                              {"name": "sea surface height"},
                              {"name": "Black Sea"},
                              {"name": "anomalies"},
                              {"name": "sea"},
                              {"name": "observation"}])
        
        elif self.harvester_type == 'bs_phy':
            tags_list.extend([{"name": "temperature"},
                              {"name": "Physics"},
                              {"name": "salinity"},
                              {"name": "sea surface height"},
                              {"name": "sea water velocity"},
                              {"name": "black sea"},
                              {"name": "Analysis"},
                              {"name": "sea"},
                              {"name": "observation"}])
        
        elif self.harvester_type == 'bs_bio':
            tags_list.extend([{"name": "chlorophyll"},
                              {"name": "carbon"},
                              {"name": "nitrate"},
                              {"name": "phosphate"},
                              {"name": "Black Sea"},
                              {"name": "biogeochemistry"},
                              {"name": "carbon dioxide"},
                              {"name": "sea"},
                              {"name": "observation"}])
        
        elif self.harvester_type == 'bs_wav':
            tags_list.extend([{"name": "Waves"},
                              {"name": "Forecast"},
                              {"name": "Black Sea"},
                              {"name": "Analysis"},
                              {"name": "sea"},
                              {"name": "significant height"},
                              {"name": "mean period"},
                              {"name": "swell"},
                              {"name": "wave"}])
        
        else:
            tags_list.extend([{"name": "sea ice"},
                              {"name": "ice"},
                              {"name": "sea"},
                              {"name": "ease"},
                              {"name": "polstere"},
                              {"name": "polar stereographic"},
                              {"name": "sea ice concentration"},
                              {"name": "observation"}])

            if self.harvester_type == 'sic_north':
                tags_list.extend([{"name": "north"},
                                  {"name": "northern"},
                                  {"name": "arctic"},
                                  {"name": "arctic ocean"}])

            elif self.harvester_type == 'sic_south':
                tags_list.extend([{"name": "south"},
                                  {"name": "Southern"},
                                  {"name": "antarctic"},
                                  {"name": "antarctic ocean"}])

        return tags_list

    # Required by NextGEOSS base harvester
    def _parse_content(self, content):
        """
        Parse the entry content and return a dictionary using our standard
        metadata terms.
        """
        self.harvester_type = self.source_config['harvester_type']

        content = json.loads(content)
        ftp_link = content['ftp_link']
        start_date = deserialize_date(content['start_date'])
        day, month, year = self._format_date_separed(start_date)
        forecast_date = content.get('forecast_date')
        if forecast_date:
            forecast_date = deserialize_date(content['forecast_date'])
            fday, fmonth, fyear = self._format_date_separed(forecast_date)

        start_date_string = str(start_date.date())

        spatial_template = '{{"type":"Polygon", "coordinates":[{}]}}'
        metadata = {}

        if self.harvester_type == 'sst':
            metadata['collection_id'] = ('METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2')
            metadata['title'] = "Global Observed Sea Surface Temperature"
            metadata['notes'] = ("Daily analysis of sea surface temperature (SST),"  # noqa: E501
                                 " based on measurements from several satellite and"  # noqa: E501
                                 " in situ SST datasets, for the global ocean and"  # noqa: E501
                                 " some lakes.")
            metadata['spatial'] = spatial_template.format([[-180, 90],
                                                           [180, 90],
                                                           [180, -90],
                                                           [-180, -90],
                                                           [-180, 90]])

            thumbnail = ("https://nrt.cmems-du.eu/thredds/wms"
                         "/METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2"
                         "?request=GetMap"
                         "&version=1.3.0"
                         "&layers=analysed_sst"
                         "&crs=CRS:84"
                         "&bbox=-180,-90,180,90"
                         "&WIDTH=800"
                         "&HEIGHT=800"
                         "&styles=boxfill/rainbow"
                         "&format=image/png"
                         "&time=" +
                         start_date_string +
                         "T12:00:00.000Z")

        elif self.harvester_type == 'sic_north':
            metadata['collection_id'] = ('METNO-GLO-SEAICE_CONC-NORTH-L4-NRT-OBS')  # noqa E501
            metadata['title'] = ("Arctic Ocean Observed Sea Ice"
                                 " Concentration")
            metadata['notes'] = ("Daily sea ice concentration at 10km"
                                 " resolution in polar stereographic and EASE"
                                 " grid projections covering the Northern"
                                 " Hemisphere.")
            metadata['spatial'] = spatial_template.format([[-180, 90],
                                                           [180, 90],
                                                           [180, 0],
                                                           [-180, 0],
                                                           [-180, 90]])

            polstere_url = ("ftp://mftp.cmems.met.no/Core/" +
                            "SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/" +
                            "METNO-GLO-SEAICE_CONC-NORTH-L4-NRT-OBS/" +
                            year +
                            "/" +
                            month +
                            "/" +
                            "ice_conc_nh_polstere-100_multi_" +
                            year +
                            month +
                            day +
                            "1200.nc")
            thumbnail = ("https://thredds.met.no/thredds/wms/" +
                         "sea_ice/SIW-OSISAF-GLO-SIT_SIE_SIC-OBS/" +
                         "ice_conc_north_aggregated" +
                         "?request=GetMap" +
                         "&layers=ice_conc" +
                         "&version=1.3.0" +
                         "&crs=CRS:84" +
                         "&bbox=-180,0,180,90" +
                         "&WIDTH=800" +
                         "&HEIGHT=800" +
                         "&styles=boxfill/rainbow" +
                         "&format=image/png" +
                         "&time=" +
                         start_date_string +
                         "T12:00:00.000Z")

        elif self.harvester_type == 'sic_south':
            metadata['collection_id'] = ('METNO-GLO-SEAICE_CONC-SOUTH-L4-NRT-OBS')  # noqa E501
            metadata['title'] = ("Antarctic Ocean Observed"
                                 " Sea Ice Concentration")
            metadata['notes'] = ("Daily sea ice concentration at 10km "
                                 "resolution in polar stereographic and EASE"
                                 " grid projections covering the Southern"
                                 " Hemisphere.")
            metadata['spatial'] = spatial_template.format([[-180, 0],
                                                           [180, 0],
                                                           [180, -90],
                                                           [-180, -90],
                                                           [-180, 0]])

            polstere_url = ("ftp://mftp.cmems.met.no/Core/" +  # noqa: E121
                            "SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/" +
                            "METNO-GLO-SEAICE_CONC-SOUTH-L4-NRT-OBS/" +
                            year +
                            "/" +
                            month +
                            "/" +
                            "ice_conc_sh_polstere-100_multi_" +
                            year +
                            month +
                            day +
                            "1200.nc")
            thumbnail = ("https://thredds.met.no/thredds/wms/" +
                         "sea_ice/SIW-OSISAF-GLO-SIT_SIE_SIC-OBS/" +
                         "ice_conc_south_aggregated" +
                         "?request=GetMap" +
                         "&layers=ice_conc" +
                         "&version=1.3.0" +
                         "&crs=CRS:84" +
                         "&bbox=-180,-90,180,0" +
                         "&WIDTH=800" +
                         "&HEIGHT=800" +
                         "&styles=boxfill/rainbow" +
                         "&format=image/png" +
                         "&time=" +
                         start_date_string +
                         "T12:00:00.000Z")

        elif self.harvester_type == 'ocn':
            metadata['collection_id'] = 'ARCTIC_ANALYSIS_FORECAST_PHYS_002_001_A'  # noqa E501
            metadata['title'] = "Arctic Ocean Physics Analysis and Forecast"
            metadata['notes'] = ("Daily Arctic Ocean physics analysis to provide 10"  # noqa E501
                                 " days of forecast of the 3D physical ocean,"
                                 " including temperature, salinity, sea ice"
                                 " concentration, sea ice thickness, sea ice velocity"  # noqa E501
                                 " and sea ice type.")
            metadata['BulletinDate'] = start_date_string
            metadata['ForecastDate'] = datetime.strftime(forecast_date,
                                                         '%Y-%m-%d')
            metadata['spatial'] = spatial_template.format([[-180, 90],
                                                           [180, 90],
                                                           [180, 63],
                                                           [-180, 63],
                                                           [-180, 90]])

            thumbnail = ("https://thredds.met.no/thredds/wms/" +
                         "topaz/" +
                         "dataset-topaz4-arc-1hr-myoceanv2-be" +
                         "?request=GetMap" +
                         "&version=1.3.0" +
                         "&layers=temperature" +
                         "&CRS=CRS:84" +
                         "&bbox=-180,0,180,90" +
                         "&WIDTH=800" +
                         "&HEIGHT=800" +
                         "&styles=boxfill/rainbow" +
                         "&format=image/png" +
                         "&time=" +
                         start_date_string)

        elif self.harvester_type == 'slv':
            metadata['collection_id'] = ('SEALEVEL_GLO_PHY_L4_NRT_OBSERVATIONS_008_046')  # noqa E501
            metadata['title'] = ("Global Ocean Gridded L4 Sea Surface"
                                 " Heights and Derived Variables NRT")
            metadata['notes'] = ("Daily products processed by the DUACS multimission altimeter"  # noqa E501
                                 " data processing system. The geostrophic currents are derived"  # noqa E501
                                 " from sla (geostrophic velocities anomalies, ugosa and vgosa"  # noqa E501
                                 " variables) and from adt (absolute geostrophic velicities,"  # noqa E501
                                 " ugos and vgos variables")
            metadata['spatial'] = spatial_template.format([[-180, 90],
                                                           [180, 90],
                                                           [180, -90],
                                                           [-180, -90],
                                                           [-180, 90]])

            thumbnail = ("https://nrt.cmems-du.eu/thredds/wms/" +
                         "dataset-duacs-nrt-global-merged-allsat-phy-l4" +
                         "?request=GetMap" +
                         "&service=WMS" +
                         "&version=1.3.0" +
                         "&layers=surface_geostrophic_sea_water_velocity" +
                         "&crs=CRS:84" +
                         "&bbox=-180,-90,180,90" +
                         "&WIDTH=800" +
                         "&HEIGHT=800" +
                         "&styles=vector/rainbow" +
                         "&format=image/png" +
                         "&time=" +
                         start_date_string +
                         "T00:00:00.000Z")
            slv_date = self._date_from_identifier_slv(content['identifier'])
            metadata['timerange_start'] = '{}T00:00:00.000Z'.format(slv_date)  # noqa E501
            metadata['timerange_end'] = metadata['timerange_start']

        elif self.harvester_type == 'gpaf':
            metadata['collection_id'] = 'GLOBAL_ANALYSIS_FORECAST_PHY_001_024'  # noqa E501
            metadata['title'] = "Global Ocean Physics Analysis and Forecast (Hourly)"   # noqa E501
            metadata['notes'] = (" Daily global ocean analysis and forecast system at 1/12 degree providing 10"  # noqa E501
                                 " days of 3D global ocean forecasts."
                                 " These datasets include hourly mean surface fields for sea level height,"   # noqa E501
                                 " temperature and currents (eastward sea water velocity, northward sea water velocity).")  # noqa E501

            metadata['spatial'] = spatial_template.format([[-180, 90],
                                                           [180, 90],
                                                           [180, -90],
                                                           [-180, -90],
                                                           [-180, 90]])

            gpaf_date_start, gpaf_date_end = self._date_from_identifier_gpaf(content['identifier'])   # noqa E501
            thumbnail = ("https://nrt.cmems-du.eu/thredds/wms/" +
                         "global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh" +
                         "?request=GetMap" +
                         "&service=WMS" +
                         "&version=1.3.0" +
                         "&layers=thetao" +
                         "&crs=CRS:84" +
                         "&bbox=-180,-90,180,90" +
                         "&WIDTH=800" +
                         "&HEIGHT=800" +
                         "&styles=boxfill/rainbow" +
                         "&format=image/gif" +
                         "&time=" +
                         str(gpaf_date_start) +
                         "T00:30:00.000Z" +
                         "/" +
                         str(gpaf_date_start) +
                         "T23:30:00.000Z")
            metadata['timerange_start'] = '{}T00:30:00.000Z'.format(gpaf_date_start)  # noqa E501
            metadata['timerange_end'] = '{}T23:30:00.000Z'.format(gpaf_date_start)  # noqa E501

            metadata['BulletinDate'] = str(gpaf_date_end)
            metadata['FieldDate'] = str(gpaf_date_start)

        elif self.harvester_type == 'mog':
            metadata['collection_id'] = 'MULTIOBS_GLO_PHY_NRT_015_003'  # noqa E501
            metadata['title'] = "Global Total Surface and 15m Current (Hourly)"   # noqa E501
            metadata['notes'] = (" This product is a 6 hourly NRT L4 global total velocity field at 0m and 15m."  # noqa E501
                                 " It consists of the zonal and meridional velocity at a 6h frequency and at 1/4 degree"  # noqa E501
                                 " regular grid produced on a daily basis. These total velocity fields are obtained by combining"  # noqa E501
                                 "CMEMS NRT satellite Geostrophic Surface Currents and modelled Ekman current at the surface and"  # noqa E501
                                 " 15m depth (using ECMWF NRT wind).")  # noqa E501
            metadata['spatial'] = spatial_template.format([[-180, 90],
                                                           [180, 90],
                                                           [180, -90],
                                                           [-180, -90],
                                                           [-180, 90]])

            thumbnail = ("https://nrt.cmems-du.eu/thredds/wms/" +
                         "dataset-uv-nrt-hourly" +
                         "?request=GetMap" +
                         "&service=WMS" +
                         "&version=1.3.0" +
                         "&layers=sea_water_velocity" +
                         "&crs=CRS:84" +
                         "&bbox=-180,-90,180,90" +
                         "&WIDTH=800" +
                         "&HEIGHT=800" +
                         "&styles=fancyvec/alg" +
                         "&format=image/gif" +
                         "&&time=" + start_date_string +
                         "T00:00:00.000Z" +
                         "/" +
                         start_date_string +
                         "T18:00:00.000Z")
            mog_date = self._date_from_identifier_mog(content['identifier'])
            metadata['timerange_start'] = '{}T00:00:00.000Z'.format(mog_date)  # noqa E501
            metadata['timerange_end'] = '{}T18:00:00.000Z'.format(mog_date)  # noqa E501
	
	elif self.harvester_type == 'med_phy':
            metadata['collection_id'] = ('MEDSEA_MULTIYEAR_PHY_006_004')
            metadata['title'] = "Mediterranean Sea Physics Reanalysis"
            metadata['notes'] = ("The Med MFC physical reanalysis product is generated by a numerical system composed of an hydrodynamic model, supplied by the Nucleous for European Modelling of the Ocean (NEMO) and a variational data assimilation scheme (OceanVAR) for temperature and salinity vertical profiles and satellite Sea Level Anomaly along track data. The model horizontal grid resolution is 1/24 deg (ca. 4-5 km) and the unevenly spaced vertical levels are 141. ")
            metadata['spatial'] = spatial_template.format([[-6.00, 45.98],
                                                           [36.30, 45.98],
                                                           [36.30, 30.18],
                                                           [-6.00, 30.18],
                                                           [-6.00, 45.98]])
		
            metadata['BulletinDate'] = self._bulletin_date_medeos(content['identifier'])
            
            thumbnail_cur = ("https://my.cmems-du.eu/thredds/wms/med-cmcc-cur-rean-d?request=GetMap&version=1.3.0&layers=vo&crs=CRS:84&bbox=-6.00,30.18,36.30,45.98"
                         "&WIDTH=800&HEIGHT=400&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
            thumbnail_mld = ("https://my.cmems-du.eu/thredds/wms/med-cmcc-mld-rean-d?request=GetMap&version=1.3.0&layers=mlotst&crs=CRS:84&bbox=-6.00,30.18,36.30,45.98"
                         "&WIDTH=800&HEIGHT=400&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
            thumbnail_sal = ("https://my.cmems-du.eu/thredds/wms/med-cmcc-sal-rean-d?request=GetMap&version=1.3.0&layers=so&crs=CRS:84&bbox=-6.00,30.18,36.30,45.98"
                         "&WIDTH=800&HEIGHT=400&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
            thumbnail_ssh = ("https://my.cmems-du.eu/thredds/wms/med-cmcc-ssh-rean-d?request=GetMap&version=1.3.0&layers=zos&crs=CRS:84&bbox=-6.00,30.18,36.30,45.98"
                         "&WIDTH=800&HEIGHT=400&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
            thumbnail_tem = ("https://my.cmems-du.eu/thredds/wms/med-cmcc-tem-rean-d?request=GetMap&version=1.3.0&layers=bottomT&crs=CRS:84&bbox=-6.00,30.18,36.30,45.98"
                         "&WIDTH=800&HEIGHT=400&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
            
	
	elif self.harvester_type == 'med_bio':
            metadata['collection_id'] = ('MEDSEA_MULTIYEAR_BGC_006_008')
            metadata['title'] = "Mediterranean Sea Biogechemistry Reanalysis"
            metadata['notes'] = ("The Mediterranean Sea biogeochemical reanalysis at 1/24 deg of horizontal resolution (ca. 4 km) covers the period 1999-2019 and is produced by means of the MedBFM3 model system. MedBFM3, which is run by OGS (IT), includes the transport model OGSTM v4.0 coupled with the biogeochemical flux model BFM v5 and the variational data assimilation module 3DVAR-BIO v2.1 for surface chlorophyll. MedBFM3 is offline coupled with the physical reanalysis (MEDSEA_MULTIYEAR_PHY_006_004 product run by CMCC) that provides daily forcing fields (i.e., currents, temperature, salinity, diffusivities, wind and solar radiation). The ESA-CCI database of surface chlorophyll concentration (CMEMS-OCTAC REP product) is assimilated with a weekly frequency.")
            metadata['spatial'] = spatial_template.format([[-5.54, 45.98],
                                                           [36.30, 45.98],
                                                           [36.30, 30.18],
                                                           [-5.54, 30.18],
                                                           [-5.54, 45.98]])
                                                           
            metadata['BulletinDate'] = self._bulletin_date_medeos(content['identifier'])
            
            thumbnail_bio = ("https://my.cmems-du.eu/thredds/wms/med-ogs-bio-rean-d?request=GetMap&version=1.3.0&layers=o2&crs=CRS:84&bbox=-5.54,30.18,36.30,45.98"
                         "&WIDTH=800&HEIGHT=400&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
            thumbnail_car = ("https://my.cmems-du.eu/thredds/wms/med-ogs-car-rean-d?request=GetMap&version=1.3.0&layers=ph&crs=CRS:84&bbox=-5.54,30.18,36.30,45.98"
                         "&WIDTH=800&HEIGHT=400&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
            thumbnail_co2 = ("https://my.cmems-du.eu/thredds/wms/med-ogs-co2-rean-d?request=GetMap&version=1.3.0&layers=fpco2&crs=CRS:84&bbox=-5.54,30.18,36.30,45.98"
                         "&WIDTH=800&HEIGHT=400&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
            thumbnail_nut = ("https://my.cmems-du.eu/thredds/wms/med-ogs-nut-rean-d?request=GetMap&version=1.3.0&layers=no3&crs=CRS:84&bbox=-5.54,30.18,36.30,45.98"
                         "&WIDTH=800&HEIGHT=400&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
            thumbnail_pft = ("https://my.cmems-du.eu/thredds/wms/med-ogs-pft-rean-d?request=GetMap&version=1.3.0&layers=phyc&crs=CRS:84&bbox=-5.54,30.18,36.30,45.98"
                         "&WIDTH=800&HEIGHT=400&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
                         
        elif self.harvester_type == 'bs_sst_006':
            metadata['collection_id'] = ('SST_BS_SST_L4_NRT_OBSERVATIONS_010_006')
            metadata['title'] = "Black Sea High Resolution and Ultra High Resolution Sea Surface Temperature Analysis"
            metadata['notes'] = ("For the Black Sea (BS), the CNR BS Sea Surface Temperature (SST) processing chain providess daily gap-free (L4) maps at high (HR 0.0625 deg) and ultra-high (UHR 0.01 deg) spatial resolution over the Black Sea. Remotely-sensed L4 SST datasets are operationally produced and distributed in near-real time by the Consiglio Nazionale delle Ricerche - Gruppo di Oceanografia da Satellite (CNR-GOS). These SST products are based on the nighttime images collected by the infrared sensors mounted on different satellite platforms, and cover the Southern European Seas. The CNR-GOS processing chain includes several modules, from the data extraction and preliminary quality control, to cloudy pixel removal and satellite images collating/merging. A two-step algorithm finally allows to interpolate SST data at high (HR 0.0625 deg) and ultra-high (UHR 0.01 deg) spatial resolution, applying statistical techniques. These L4 data are also used to estimate the SST anomaly with respect to a pentad climatology.")
            metadata['spatial'] = spatial_template.format([[26.37, 48.82],
                                                           [42.38, 48.82],
                                                           [42.38, 38.75],
                                                           [26.37, 38.75],
                                                           [26.37, 48.82]])

            thumbnail = ("https://nrt.cmems-du.eu/thredds/wms/SST_BS_SST_L4_NRT_OBSERVATIONS_010_006_c_V2"
                         "?request=GetMap"
                         "&version=1.3.0"
                         "&layers=analysed_sst"
                         "&crs=CRS:84"
                         "&bbox=26.37,38.75,42.38,48.82"
                         "&WIDTH=800"
                         "&HEIGHT=800"
                         "&styles=boxfill/rainbow"
                         "&format=image/png"
                         "&time=" +
                         start_date_string +
                         "T00:00:00.000Z")
                         
        elif self.harvester_type == 'bs_chl':
            metadata['collection_id'] = 'OCEANCOLOUR_BS_CHL_L3_NRT_OBSERVATIONS_009_044'  # noqa E501
            metadata['title'] = "Black Sea Surface Chlorophyll Concentration from Multi Satellite and Sentinel-3 OLCI observations"
            metadata['notes'] = ("The Global Ocean Satellite monitoring and marine ecosystem study group (GOS) of the Italian National Research Council (CNR), in Rome, distributes surface chlorophyll concentration (mg m-3) derived from multi-sensor (MODIS-AQUA, NOAA20-VIIRS, NPP-VIIRS, Sentinel3A-OLCI at 300m of resolution) (at 1 km resolution) and Sentinel3A-OLCI (at 300m resolution) Rrs spectra. The chlorophyll (Chl) product of the Black Sea is obtained combining two different regional algorithms (BSAlg). The first is a band-ratio algorithm (B/R) (Zibordi et al., 2015) that computes Chl as a function of the slope of Rrs values at two wavelengths (490 and 555 nm) using a polynomial regression that captures the overall data trend. The second one is a Multilayer Perceptron (MLP) neural net based on Rrs values at three individual wavelengths (490, 510 and 555 nm) that features interpolation capabilities helpful to fit data nonlinearities. The merging scheme (Kajiyama et al., 2018) has been designed to use the B/R algorithm and the MLP neural net in waters exhibiting lower and higher optical complexity, respectively. For multi-sensor observations, single sensor Rrs fields are band-shifted, over the SeaWiFS native bands (using the QAAv6 model, Lee et al., 2002) and merged with a technique aimed at smoothing the differences among different sensors. The current day data temporal consistency is evaluated as Quality Index (QI):QI=(CurrentDataPixel-ClimatologyDataPixel)/STDDataPixelwhere QI is the difference between current data and the relevant climatological field as a signed multiple of climatological standard deviations (STDDataPixel).")
            
            metadata['spatial'] = spatial_template.format([[26.50, 48.00 ],
                                                           [42.00, 48.00 ],
                                                           [42.00, 40.00],
                                                           [26.50, 40.00],
                                                           [26.50, 48.00 ]])

            thumbnail = ("https://nrt.cmems-du.eu/thredds/wms/dataset-oc-bs-chl-multi-l3-chl_1km_daily-rt-v02" +
                         "?request=GetMap" +
                         "&version=1.3.0" +
                         "&layers=CHL" +
                         "&CRS=CRS:84" +
                         "&bbox=26.50,40.00,42.00,48.00" +
                         "&WIDTH=800" +
                         "&HEIGHT=800" +
                         "&styles=boxfill/rainbow" +
                         "&format=image/png" +
                         "&time=" +
                         start_date_string + "T00:00:00.000Z")
        
        elif self.harvester_type == 'bs_opt':
            metadata['collection_id'] = 'OCEANCOLOUR_BS_OPTICS_L3_NRT_OBSERVATIONS_009_042'  # noqa E501
            metadata['title'] = "Black Sea Remote Sensing Reflectances and Attenuation Coefficient at 490nm from Multi Satellite and Sentinel-3 OLCI observations"   # noqa E501
            metadata['notes'] = ("The Global Ocean Satellite monitoring and marine ecosystem study group (GOS) of the Italian National Research Council (CNR), in Rome, distributes Remote Sensing Reflectances (Rrs), and diffuse attenuation coefficient of light at 490 nm (kd490) for multi-sensor (MODIS-AQUA, NOAA20-VIIRS, NPP-VIIRS, Sentinel3A-OLCI at 300m of resolution) (at 1 km resolution) and Sentinel3A-OLCI observations (at 300m resolution). Exclusively for multi-sensor also the absorption of phytoplankton (aph443), Gelbstoff material (adg443), and the particulate backscattering (bbp443) coefficients at 443 nm are provided. Rrs is defined as the ratio of upwelling radiance and downwelling irradiance at any wavelength (412, 443, 490, 555, and 670 nm for multi-sensor, and 400, 412, 443, 490, 510, 560, 620, 665, 674, 681 and 709 nm for OLCI) and can also be expressed as the ratio of normalized water leaving Radiance (nLw) and the extra-terrestrial solar irradiance (F0). Kd490 is defined as the diffuse attenuation coefficient of light at 490 nm, and is a measure of the turbidity of the water column. It is related to the presence of scattering particles via the ratio between Rrs at 490 and 555 nm (490 and 560 nm for OLCI). The current day data temporal consistency is evaluated as Quality Index (QI):QI=(CurrentDataPixel-ClimatologyDataPixel)/STDDataPixelwhere QI is the difference between current data and the relevant climatological field as a signed multiple of climatological standard deviations (STDDataPixel).Inherent Optical Properties (aph443, adg443 and bbp443 at 443nm) are derived via QAAv6 model.")  # noqa E501
            metadata['spatial'] = spatial_template.format([[26.50, 48.00],
                                                           [42.00, 48.00],
                                                           [42.00, 40.00],
                                                           [26.50, 40.00],
                                                           [26.50, 48.00]])

            thumbnail = ("https://nrt.cmems-du.eu/thredds/wms/dataset-oc-bs-opt-multi-l3-adg443_1km_daily-rt-v02" +
                         "?request=GetMap" +
                         "&service=WMS" +
                         "&version=1.3.0" +
                         "&layers=ADG443" +
                         "&crs=CRS:84" +
                         "&bbox=26.50,40.00,42.00,48.00" +
                         "&WIDTH=800" +
                         "&HEIGHT=800" +
                         "&styles=boxfill/occam" +
                         "&format=image/png" +
                         "&&time=" + start_date_string +
                         "T00:00:00.000Z")
        
        elif self.harvester_type == 'bs_phy_l4':
            metadata['collection_id'] = ('SEALEVEL_BS_PHY_L4_REP_OBSERVATIONS_008_042')
            metadata['title'] = "Black Sea Gridded L4 Sea Level Anomalies and Derived Variables Reprocessed (1993-Ongoing)"
            metadata['notes'] = ("Altimeter satellite gridded Sea Level Anomalies (SLA) computed with respect to a twenty-year 2012 mean. The SLA is estimated by Optimal Interpolation, merging the measurement from the different altimeter missions available (see QUID document or http://duacs.cls.fr pages for processing details). The product gives additional variables (i.e. geostrophic currents (anomalies)).This product is processed by the DUACS multimission altimeter data processing system. It serves in near-real time the main operational oceanography and climate forecasting centers in Europe and worldwide. It processes data from all altimeter missions: Jason-3, Sentinel-3A, HY-2A, Saral/AltiKa, Cryosat-2, Jason-2, Jason-1, T/P, ENVISAT, GFO, ERS1/2. It provides a consistent and homogeneous catalogue of products for varied applications, both for near real time applications and offline studies. To produce maps of Sea Level Anomalies (SLA) in delayed-time (REPROCESSED), the system uses the along-track altimeter missions from products called SEALEVEL*_PHY_L3_REP_OBSERVATIONS_008_*. Finally an Optimal Interpolation is made merging all the flying satellites in order to compute gridded SLA. The geostrophic currents are derived from sla (geostrophic velocities anomalies, ugosa and vgosa variables). Note that the gridded products can be visualized on the LAS (Live Access Data) Aviso+ web page (http://www.aviso.altimetry.fr/en/data/data-access/las-live-access-server.html)")
            metadata['spatial'] = spatial_template.format([[27.00, 47.00],
                                                           [42.00, 47.00],
                                                           [42.00, 40.00],
                                                           [27.00, 40.00],
                                                           [27.00, 47.00]])

            thumbnail = ("https://my.cmems-du.eu/thredds/wms/dataset-duacs-rep-blacksea-merged-allsat-phy-l4"
                         "?request=GetMap"
                         "&version=1.3.0"
                         "&layers=sla"
                         "&crs=CRS:84"
                         "&bbox=27.00,40.00,42.00,47.00"
                         "&WIDTH=800"
                         "&HEIGHT=800"
                         "&styles=boxfill/rainbow"
                         "&format=image/png"
                         "&time=" +
                         start_date_string +
                         "T00:00:00.000Z")
        
        elif self.harvester_type == 'bs_phy':
            metadata['collection_id'] = ('BLKSEA_ANALYSISFORECAST_PHY_007_001')
            metadata['title'] = "Black Sea Physics Analysis and Forecast"
            metadata['notes'] = ("The Black Sea Physical Analysis and Forecast System, called BS-PHY NRT (version EAS4), provides operational ocean fields for the Black Sea basin, with online timeseries starting since 2019. The hydrodynamical core is based on NEMO general circulation ocean model (version 4.0, Madec et al., 2019), implemented in the BS domain and including a portion of the Marmara Sea, with horizontal resolution of 1/40 degrees x 1/40 degrees and 121 vertical levels. NEMO is forced by atmospheric surface fluxes computed by bulk formulation using ECMWF-IFS atmospheric fields at the available resolution in space and time. BS-PHY NRT implements open boundary conditions at the Marmara Sea box for the optimal interface with the Mediterranean Sea, by using high resolution model solutions provided by the Unstructured Turkish Strait System (U-TSS). The model is online coupled to OceanVar assimilation scheme (Dobricic and Pinardi, 2008; Storto et al., 2015) to assimilate temperature/salinity observed profiles, sea level anomaly along-track observations and satellite sea surface temperature provided by CMEMS TACs.")
            metadata['spatial'] = spatial_template.format([[27.25, 47.00],
                                                           [41.10, 47.00],
                                                           [41.10, 40.50],
                                                           [27.25, 40.50],
                                                           [27.25, 47.00]])

            metadata['BulletinDate'] = datetime.strftime(forecast_date,
                                                         '%Y-%m-%d')
            thumbnail_cur = ("https://nrt.cmems-du.eu/thredds/wms/bs-cmcc-cur-an-fc-d?request=GetMap&version=1.3.0&layers=uo&crs=CRS:84&bbox=27.25,40.50,41.10,47.00"
                         "&WIDTH=800&HEIGHT=600&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
            thumbnail_sal = ("https://nrt.cmems-du.eu/thredds/wms/bs-cmcc-sal-an-fc-d?request=GetMap&version=1.3.0&layers=so&crs=CRS:84&bbox=27.25,40.50,41.10,47.00"
                         "&WIDTH=800&HEIGHT=600&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
            thumbnail_tem = ("https://nrt.cmems-du.eu/thredds/wms/bs-cmcc-tem-an-fc-d?request=GetMap&version=1.3.0&layers=bottomT&crs=CRS:84&bbox=27.25,40.50,41.10,47.00"
                         "&WIDTH=800&HEIGHT=600&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
            thumbnail_ssh = ("https://nrt.cmems-du.eu/thredds/wms/bs-cmcc-ssh-an-fc-d?request=GetMap&version=1.3.0&layers=zos&crs=CRS:84&bbox=27.25,40.50,41.10,47.00"
                         "&WIDTH=800&HEIGHT=600&styles=boxfill/rainbow&format=image/png&time=" +start_date_string +"T12:00:00.000Z")
        
        elif self.harvester_type == 'bs_bio':
            metadata['collection_id'] = ('BLKSEA_ANALYSIS_FORECAST_BIO_007_010')
            metadata['title'] = "Black Sea Biogeochemistry Analysis and Forecast"
            metadata['notes'] = ("BLKSEA_ANALYSIS_FORECAST_BIO_007_010 is the nominal product of the Black Sea Biogeochemistry NRT system and is generated by the NEMO-BAMHBI modelling system. Biogeochemical Model for Hypoxic and Benthic Influenced areas (BAMHBI) is an innovative biogeochemical model with a 28-variable pelagic component (including the carbonate system) and a 6-variable benthic component ; it explicitely represents processes in the anoxic layer.The product provides analysis and forecast for 3D concentration of chlorophyll, nutrients (nitrate and phosphate), dissolved oxygen, phytoplankton carbon biomass, net primary production, pH, dissolved inorganic carbon, total alkalinity, and for 2D fields of bottom oxygen concentration (for the North-Western shelf), surface partial pressure of CO2 and surface flux of CO2. These variables are computed on the same grid as the PHY product, at ~3km x 31-levels resolution, and are provided as daily and monthly means.")
            metadata['BulletinDate'] = datetime.strftime(forecast_date,
                                                         '%Y-%m-%d')
            
            metadata['spatial'] = spatial_template.format([[27.37, 46.80],
                                                           [41.96, 46.80],
                                                           [41.96, 40.86],
                                                           [27.37, 40.86],
                                                           [27.37, 46.80]])

            thumbnail = ("https://nrt.cmems-du.eu/thredds/wms/bs-ulg-pft-an-fc-d"
                         "?request=GetMap"
                         "&version=1.3.0"
                         "&layers=chl"
                         "&crs=CRS:84"
                         "&bbox=27.37,40.86,41.96,46.80"
                         "&WIDTH=800"
                         "&HEIGHT=800"
                         "&styles=boxfill/rainbow"
                         "&format=image/png"
                         "&time=" +
                         start_date_string +
                         "T12:00:00.000Z")
        
        elif self.harvester_type == 'bs_wav':
            metadata['collection_id'] = ('BLKSEA_ANALYSISFORECAST_WAV_007_003')
            metadata['title'] = "Black Sea Waves Analysis and Forecast"
            metadata['notes'] = ("The third generation spectral wave model WAM Cycle 6 has been adapted to the Black Sea area und runs successfully on the Cluster at HZG. The shallow water version is implemented on a spherical grid with a spatial resolution of about 3 km (133 * 100 sec) with 24 directional and 30 frequency bins. The number of active wave model grid points is 44699. The model takes into account depth refraction and wave breaking and provides currently a ten days forecast with one-hourly output once a day. The atmospheric forcing is taken from ECMWF in NetCDF and locally (on HZG server) transformed into WAM format forcing. Surface currents from BLKSEA_ANALYSIS_FORECAST_PHYS_007_001 are one-way coupled and taken into account in WAM.")
            metadata['spatial'] = spatial_template.format([[27.32, 46.80],
                                                           [41.96, 46.80],
                                                           [41.96, 40.86],
                                                           [27.32, 40.86],
                                                           [27.32, 46.80]])
            
            thumbnail = ("https://nrt.cmems-du.eu/thredds/wms/bs-hzg-wav-an-fc-h"
                         "?request=GetMap"
                         "&version=1.3.0"
                         "&layers=VTM10"
                         "&crs=CRS:84"
                         "&bbox=27.32,40.86,41.96,46.80"
                         "&WIDTH=800"
                         "&HEIGHT=800"
                         "&styles=boxfill/rainbow"
                         "&format=image/png"
                         "&time=" +
                         start_date_string +
                         "T12:00:00.000Z")
        
                              
        # Add common metadata
        metadata['identifier'] = content['identifier']
        metadata['name'] = metadata['identifier'].lower().replace('.','_')
        if self.harvester_type not in ('slv', 'gpaf', 'mog'):
            metadata['timerange_start'] = '{}T00:00:00.000Z'.format(start_date_string)  # noqa E501
            metadata['timerange_end'] = self._make_stop_time(start_date)

        # For now, the collection name and description are the same as the
        # title and notes, though one or the other should probably change in
        # the future.
        metadata['collection_name'] = metadata['title']
        metadata['collection_description'] = metadata['notes']

        metadata['tags'] = self._create_tags()
        size = content['size']

        resources = []

        if self.harvester_type in {'sst', 'ocn', 'slv', 'gpaf', 'mog','bs_sst_006','bs_chl','bs_opt','bs_phy_l4','bs_bio','bs_wav'}:
            resources.append(self._make_resource(ftp_link,
                                                 'Product Download',
                                                 size))
            resources.append(self._make_resource(thumbnail,'Thumbnail Link'))       
        
        elif self.harvester_type=='med_bio':
            ftp_link_bio = ftp_link
            ftp_link_car = ftp_link.replace('bio','car').replace('BIOL','CARB')
            ftp_link_co2 = ftp_link.replace('bio','co2').replace('BIOL','CO2F')
            ftp_link_nut = ftp_link.replace('bio','nut').replace('BIOL','NUTR')
            ftp_link_pft = ftp_link.replace('bio','pft').replace('BIOL','PFTC')
            resources.append(self._make_resource(ftp_link_bio,'Product Download (bio-rean-d)',size))
            resources.append(self._make_resource(ftp_link_car,'Product Download (car-rean-d)',size))
            resources.append(self._make_resource(ftp_link_co2,'Product Download (co2-rean-d)',size))
            resources.append(self._make_resource(ftp_link_nut,'Product Download (nut-rean-d)',size))
            resources.append(self._make_resource(ftp_link_pft,'Product Download (pft-rean-d)',size))
            resources.append(self._make_resource(thumbnail_bio,'Thumbnail Link (bio-rean-d)'))
            resources.append(self._make_resource(thumbnail_car,'Thumbnail Link (car-rean-d)'))
            resources.append(self._make_resource(thumbnail_co2,'Thumbnail Link (co2-rean-d)'))
            resources.append(self._make_resource(thumbnail_nut,'Thumbnail Link (nut-rean-d)'))
            resources.append(self._make_resource(thumbnail_pft,'Thumbnail Link (pft-rean-d)'))
        
        elif self.harvester_type=='med_phy':
            ftp_link_cur = ftp_link
            ftp_link_mld = ftp_link.replace('cur','mld').replace('RFVL','AMXL')
            ftp_link_sal = ftp_link.replace('cur','sal').replace('RFVL','PSAL')
            ftp_link_ssh = ftp_link.replace('cur','ssh').replace('RFVL','ASLV')
            ftp_link_tem = ftp_link.replace('cur','tem').replace('RFVL','TEMP')
            resources.append(self._make_resource(ftp_link_cur,'Product Download (cur-rean-d)',size))
            resources.append(self._make_resource(ftp_link_mld,'Product Download (mld-rean-d)',size))
            resources.append(self._make_resource(ftp_link_sal,'Product Download (sal-rean-d)',size))
            resources.append(self._make_resource(ftp_link_ssh,'Product Download (ssh-rean-d)',size))
            resources.append(self._make_resource(ftp_link_tem,'Product Download (tem-rean-d)',size))
            resources.append(self._make_resource(thumbnail_cur,'Thumbnail Link (cur-rean-d)'))
            resources.append(self._make_resource(thumbnail_mld,'Thumbnail Link (mld-rean-d)'))
            resources.append(self._make_resource(thumbnail_sal,'Thumbnail Link (sal-rean-d)'))
            resources.append(self._make_resource(thumbnail_ssh,'Thumbnail Link (ssh-rean-d)'))
            resources.append(self._make_resource(thumbnail_tem,'Thumbnail Link (tem-rean-d)'))
            
        elif self.harvester_type=='bs_phy':
            ftp_link_cur = ftp_link
            ftp_link_sal = ftp_link.replace('cur','sal').replace('RFVL','PSAL')
            ftp_link_tem = ftp_link.replace('cur','tem').replace('RFVL','TEMP')
            ftp_link_ssh = ftp_link.replace('cur','ssh').replace('RFVL','ASLV')
            resources.append(self._make_resource(ftp_link_cur,'Product Download (cur)',size))
            resources.append(self._make_resource(ftp_link_sal,'Product Download (sal)',size))
            resources.append(self._make_resource(ftp_link_tem,'Product Download (tem)',size))
            resources.append(self._make_resource(ftp_link_ssh,'Product Download (ssh)',size))
            resources.append(self._make_resource(thumbnail_cur,'Thumbnail Link (cur)'))
            resources.append(self._make_resource(thumbnail_sal,'Thumbnail Link (sal)'))
            resources.append(self._make_resource(thumbnail_tem,'Thumbnail Link (tem)'))
            resources.append(self._make_resource(thumbnail_ssh,'Thumbnail Link (ssh)'))
        
        else:
            resources.append(self._make_resource(ftp_link,
                                                 'Product Download (EASE GRID)',  # noqa: E501
                                                 size))
            resources.append(self._make_resource(polstere_url,
                                                 'Product Download (Polar Stereographic)'))  # noqa: E501
	    resources.append(self._make_resource(thumbnail,'Thumbnail Link'))
        
        metadata['resource'] = resources

        return metadata

    # Required by NextGEOSS base harvester
    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        return metadata['resource']

    def _make_resource(self, url, name, size=None):
        """Return a resource dictionary."""
        resource_dict = {}
        resource_dict['name'] = name
        resource_dict['url'] = url
        if name.split(' ')[0] == 'Thumbnail':
            resource_dict['format'] = 'png'
            resource_dict['mimetype'] = 'image/png'
        else:
            resource_dict['format'] = 'netcdf'
            resource_dict['mimetype'] = 'application/x-netcdf'
            resource_dict['description'] = ('Download the netCDF'
                                            ' from CMEMS. NOTE:'
                                            ' DOWNLOAD REQUIRES'
                                            ' LOGIN')

        if size:
            resource_dict['size'] = size

        return resource_dict

    def convert_date_config(self, term):
        """Convert a term into a datetime object."""
        if term == 'YESTERDAY':
            date_time = datetime.now() - timedelta(days=1)
        elif term in {'TODAY', 'NOW'}:
            date_time = datetime.now()

        return date_time.replace(hour=0, minute=0, second=0, microsecond=0)


def deserialize_date(date_string):
    """Deserialize dates serialized by calling str(date)."""
    return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
