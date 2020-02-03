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

    def _create_object(self, identifier, ftp_link, size, forecast_date):

        extras = [HOExtra(key='status',
                          value='new')]

        content = json.dumps({'identifier': identifier, 'ftp_link': ftp_link,
                              'size': size, 'start_date': self.start_date,
                              'forecast_date': forecast_date}, default=str)

        obj = HarvestObject(job=self.job, guid=unicode(uuid.uuid4()),
                            extras=extras, content=content)

        obj.save()

        return obj.id

    def _get_products(self):
        """
        Check if a product or products exist on an FTP server, create a harvest
        object and append the id to a list if so.

        The `ocn` source is a special case, with multiple products and
        additional date parameters.
        """
        day, month, year = self._format_date_separed(self.start_date)

        harvest_object_ids = []

        if self.harvester_type == 'ocn':
            num_products = 10
        else:
            num_products = 1

        for i in range(num_products):
            if self.harvester_type == 'ocn':
                forecast_date = self.start_date + timedelta(days=i)
                fday, fmonth, fyear = self._format_date_separed(
                    forecast_date)
            else:
                forecast_date = fday = fmonth = fyear = None

            identifier = self._make_identifier(day, month,
                                               year, fday, fmonth, fyear)

            if not self._was_harvested(identifier):

                ftp_link = self._make_ftp_link(day, month,
                                               year, fday, fmonth, fyear)

                size = self._crawl_urls_ftp(ftp_link, 'cmems')

                if size:
                    harvest_object_id = self._create_object(identifier,
                                                            ftp_link,
                                                            size,
                                                            forecast_date)
                    harvest_object_ids.append(harvest_object_id)

        return harvest_object_ids

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

    def _make_ftp_link(self, day, month, year, fday=None,
                       fmonth=None, fyear=None):
        """
        Construct a link to a product based on the harvest type and start date.
        """

        if self.harvester_type == 'sst':
            return ("ftp://nrt.cmems-du.eu/Core/"
                    "SST_GLO_SST_L4_NRT_OBSERVATIONS_010_001/"
                    "METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2/" +
                    year +
                    "/" +
                    month +
                    "/" +
                    year +
                    month +
                    day +

                    "120000-UKMO-L4_GHRSST-SSTfnd-OSTIA-GLOB-v02.0-fv02.0.nc")

        elif self.harvester_type == 'sic_north':
            return ("ftp://mftp.cmems.met.no/Core/"
                    "SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/"
                    "METNO-GLO-SEAICE_CONC-NORTH-L4-NRT-OBS/" +
                    year +
                    "/" +
                    month +
                    "/" +
                    "ice_conc_nh_ease-125_multi_" +
                    year +
                    month +
                    day +
                    "1200.nc")

        elif self.harvester_type == 'sic_south':
            return ("ftp://mftp.cmems.met.no/Core/"
                    "SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/"
                    "METNO-GLO-SEAICE_CONC-SOUTH-L4-NRT-OBS/" +
                    year +
                    "/" +
                    month +
                    "/" +
                    "ice_conc_sh_ease-125_multi_" +
                    year +
                    month +
                    day +
                    "1200.nc")

        elif self.harvester_type == 'ocn':
            return ("ftp://mftp.cmems.met.no/Core/"
                    "ARCTIC_ANALYSIS_FORECAST_PHYS_002_001_a/"
                    "dataset-topaz4-arc-myoceanv2-be/" +
                    fyear +
                    fmonth +
                    fday +
                    "_dm-metno-MODEL-topaz4-ARC-b" +
                    year +
                    month +
                    day +
                    "-fv02.0.nc")

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

    def _get_products_ftp_dir(self, year, month):
        harvest_object_ids = list()
        try:
            ftp = self._connect_ftp(year, month)
            for filename in ftp.nlst():
                identifier = path.splitext(filename)[0]

                if not self._was_harvested(identifier):
                    ftp_link = self._make_ftp_link_ftp_dir(year, month, identifier)  # noqa: E501

                    harvest_object_id = self._create_object(identifier,
                                                            ftp_link,
                                                            0,
                                                            None)
                    harvest_object_ids.append(harvest_object_id)
        except Ftp5xxErrors:
            pass
        return harvest_object_ids

    def _make_ftp_link_ftp_dir(self, year, month, identifier):
        if self.harvester_type == 'slv':
            link = ('ftp:///Core/'
                    'SEALEVEL_GLO_PHY_L4_NRT_OBSERVATIONS_008_046/'
                    'dataset-duacs-nrt-global-merged-allsat-phy-l4/'
                    '{}/{}/{}.nc').format(year, month, identifier)
        if self.harvester_type == 'gpaf':
            link = ('ftp:///Core/'
                    'GLOBAL_ANALYSIS_FORECAST_PHY_001_024/'
                    'global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh/'
                    '{}/{}/{}.nc').format(year, month, identifier)
        if self.harvester_type == 'mog':
            link = ('ftp:///Core/'
                    'MULTIOBS_GLO_PHY_NRT_015_003/dataset-uv-nrt-hourly/'
                    '{}/{}/{}.nc').format(year, month, identifier)
        if self.harvester_type == 'sst':
            link = ('ftp://nrt.cmems-du.eu/Core/'
                    'SST_GLO_SST_L4_NRT_OBSERVATIONS_010_001/'
                    'METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2/'
                    '{}/{}/{}.nc').format(year, month, identifier)
        if self.harvester_type == 'sic_north':
            link = ('ftp://mftp.cmems.met.no/Core/'
                    'SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/'
                    'METNO-GLO-SEAICE_CONC-NORTH-L4-NRT-OBS/'
                    '{}/{}/{}.nc').format(year, month, identifier)
        if self.harvester_type == 'sic_south':
            link = ('ftp://mftp.cmems.met.no/Core/'
                    'SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/'
                    'METNO-GLO-SEAICE_CONC-SOUTH-L4-NRT-OBS/'
                    '{}/{}/{}.nc').format(year, month, identifier)
        if self.harvester_type == 'ocn':
            link = ('ftp://mftp.cmems.met.no/Core/'
                    'ARCTIC_ANALYSIS_FORECAST_PHYS_002_001_a/'
                    'dataset-topaz4-arc-myoceanv2-be/'
                    '{}/{}/{}.nc').format(year, month, identifier)
        return link

    def _create_months_years_list(self):
        dates_list = list()

        current_date = self.start_date - timedelta(days=31)
        while current_date < self.end_date:
            dates_list.append((current_date.strftime('%Y'),
                               current_date.strftime('%m')))
            current_date += relativedelta(months=1)
        return dates_list

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

    def _connect_ftp(self, year, month):
        ftp = FTP('')

        username = self.source_config['username']
        password = self.source_config['password']

        ftp.login(username, password)

        if self.harvester_type == 'slv':
            directory = ('/Core/'
                         'SEALEVEL_GLO_PHY_L4_NRT_OBSERVATIONS_008_046/'
                         'dataset-duacs-nrt-global-merged-allsat-phy-l4/'
                         '{}/{}').format(year, month)
        elif self.harvester_type == 'gpaf':
            directory = ('/Core/'
                         'GLOBAL_ANALYSIS_FORECAST_PHY_001_024/'
                         'global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh/'  # noqa: E501
                         '{}/{}').format(year, month)
        elif self.harvester_type == 'mog':
            directory = ('/Core/'
                         'MULTIOBS_GLO_PHY_NRT_015_003/'
                         'dataset-uv-nrt-hourly'
                         '{}/{}').format(year, month)
        elif self.harvester_type == 'sst':
            directory = ('ftp://nrt.cmems-du.eu/Core/'
                         'SST_GLO_SST_L4_NRT_OBSERVATIONS_010_001/'
                         'METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2/'
                         '{}/{}').format(year, month)
        elif self.harvester_type == 'sic_north':
            directory = ('ftp://mftp.cmems.met.no/Core/'
                         'SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/'
                         'METNO-GLO-SEAICE_CONC-NORTH-L4-NRT-OBS/'
                         '{}/{}').format(year, month)
        elif self.harvester_type == 'sic_south':
            directory = ('ftp://mftp.cmems.met.no/Core/'
                         'SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/'
                         'METNO-GLO-SEAICE_CONC-SOUTH-L4-NRT-OBS/'
                         '{}/{}').format(year, month)
        elif self.harvester_type == 'ocn':
            directory = ('ftp://mftp.cmems.met.no/Core/'
                         'ARCTIC_ANALYSIS_FORECAST_PHYS_002_001_a/'
                         'dataset-topaz4-arc-myoceanv2-be/'
                         '{}/{}').format(year, month)
        ftp.cwd(directory)
        return ftp

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

        resources = []

        if self.harvester_type in {'sst', 'ocn', 'slv', 'gpaf', 'mog'}:
            resources.append(self._make_resource(ftp_link,
                                                 'Product Download'))
        else:
            resources.append(self._make_resource(ftp_link,
                                                 'Product Download (EASE GRID)'))  # noqa: E501
            resources.append(self._make_resource(polstere_url,
                                                 'Product Download (Polar Stereographic)'))  # noqa: E501

        resources.append(self._make_resource(thumbnail,
                                             'Thumbnail Link'))
        metadata['resource'] = resources

        return metadata

    def _make_identifier(self, day, month, year,
                         fday=None, fmonth=None, fyear=None):
        """
        Make an identifier for the product according to how it's identified at
        the source.
        """
        if self.harvester_type == 'sst':
            return 'SST-GLO-L4-DAILY-NRT-OBS-010-001-{}{}{}'.format(year,
                                                                    month,
                                                                    day)
        elif self.harvester_type == 'sic_north':
            return 'SEAICE-CONC-NORTH-L4-DAILY-NRT-OBS-011-001-{}{}{}'.format(
                year, month, day)
        elif self.harvester_type == 'sic_south':
            return 'SEAICE-CONC-SOUTH-L4-DAILY-NRT-OBS-011-001{}{}{}'.format(
                year, month, day)
        elif self.harvester_type == 'ocn':
            return 'ARCTIC-FORECAST-{}{}{}-PHYS-002-001-{}{}{}'.format(
                fyear, fmonth, fday, year, month, day)

    # Required by NextGEOSS base harvester
    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        return metadata['resource']

    def _make_resource(self, url, name):
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
