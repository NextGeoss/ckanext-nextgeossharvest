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
        
        elif self.harvester_type == 'bs_phy_l4':
            tags_list.extend([{"name": "PHY"},
                              {"name": "sea anomalies"},
                              {"name": "Black Sea"},
                              {"name": "sea observation"},
                              {"name": "sea"},
                              {"name": "observation"}])
        
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

            thumbnail = ("http://nrt.cmems-du.eu/thredds/wms"
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
            thumbnail = ("http://thredds.met.no/thredds/wms/" +
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
            thumbnail = ("http://thredds.met.no/thredds/wms/" +
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

            thumbnail = ("http://thredds.met.no/thredds/wms/" +
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

            thumbnail = ("http://nrt.cmems-du.eu/thredds/wms/" +
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
            thumbnail = ("http://nrt.cmems-du.eu/thredds/wms/" +
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

            thumbnail = ("http://nrt.cmems-du.eu/thredds/wms/" +
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

        elif self.harvester_type == 'bs_phy_l4':
            metadata['collection_id'] = ('SEALEVEL_BS_PHY_L4_REP_OBSERVATIONS_008_042')
            metadata['title'] = "BLACK SEA GRIDDED L4 SEA LEVEL ANOMALIES AND DERIVED VARIABLES REPROCESSED (1993-ONGOING)"
            metadata['notes'] = ("Altimeter satellite gridded Sea Level Anomalies (SLA) computed with respect to a twenty-year 2012 mean. The SLA is estimated by Optimal Interpolation, merging the measurement from the different altimeter missions available (see QUID document or http://duacs.cls.fr [1] pages for processing details). The product gives additional variables (i.e. geostrophic currents (anomalies)).This product is processed by the DUACS multimission altimeter data processing system. It serves in near-real time the main operational oceanography and climate forecasting centers in Europe and worldwide. It processes data from all altimeter missions: Jason-3, Sentinel-3A, HY-2A, Saral/AltiKa, Cryosat-2, Jason-2, Jason-1, T/P, ENVISAT, GFO, ERS1/2. It provides a consistent and homogeneous catalogue of products for varied applications, both for near real time applications and offline studies. To produce maps of Sea Level Anomalies (SLA) in delayed-time (REPROCESSED), the system uses the along-track altimeter missions from products called SEALEVEL_PHY_L3_REP_OBSERVATIONS_008_. Finally an Optimal Interpolation is made merging all the flying satellites in order to compute gridded SLA. The geostrophic currents are derived from sla (geostrophic velocities anomalies, ugosa and vgosa variables). Note that the gridded products can be visualized on the LAS (Live Access Data) Aviso+ web page (http://www.aviso.altimetry.fr/en/data/data-access/las-live-access-server.html [2])")
            metadata['spatial'] = spatial_template.format([[27.00, 47.00],
                                                           [42.00, 47.00],
                                                           [42.00, 40.00],
                                                           [27.00, 40.00],
                                                           [27.00, 47.00]])

            metadata['ForecastDate'] = datetime.strftime(forecast_date,
                                                         '%Y-%m-%d')
            thumbnail = ("https://my.cmems-du.eu/thredds/wms/dataset-duacs-rep-blacksea-merged-allsat-phy-l4"
                         "?request=GetMap"
                         "&version=1.3.0"
                         "&layers=err"
                         "&crs=CRS:84"
                         "&bbox=27.00,40.00,42.00,47.00"
                         "&WIDTH=800"
                         "&HEIGHT=800"
                         "&styles=boxfill/rainbow"
                         "&format=image/png"
                         "&time=" +
                         start_date_string +
                         "T00:00:00.000Z")
        
        
        # Add common metadata
        metadata['identifier'] = content['identifier']
        metadata['name'] = metadata['identifier'].lower()
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

        if self.harvester_type in {'sst', 'ocn', 'slv', 'gpaf', 'mog','bs_phy_l4'}:
            resources.append(self._make_resource(ftp_link,
                                                 'Product Download',
                                                 size))
        else:
            resources.append(self._make_resource(ftp_link,
                                                 'Product Download (EASE GRID)',  # noqa: E501
                                                 size))
            resources.append(self._make_resource(polstere_url,
                                                 'Product Download (Polar Stereographic)'))  # noqa: E501

        resources.append(self._make_resource(thumbnail,
                                             'Thumbnail Link'))
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
        if name == 'Thumbnail Link':
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
