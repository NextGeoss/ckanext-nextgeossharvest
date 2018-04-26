# -*- coding: utf-8 -*-

import logging
import json
from datetime import timedelta, datetime
import uuid

from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.model import HarvestObject


log = logging.getLogger(__name__)


class CMEMSBase(HarvesterBase):

    def _create_object(self, harvester_type, ftp_link,
                       start_date, forecast_date):

        extras = [HOExtra(key='status',
                          value='new')]

        content = json.dumps({'ftp_link': ftp_link, 'start_date': start_date,
                              'forecast_date': forecast_date}, default=str)

        obj = HarvestObject(job=self.job, guid=unicode(uuid.uuid4()),
                            extras=extras, content=content)

        obj.save()

        return obj.id

    def _get_products(self, harvester_type, start_date):
        day, month, year = self._format_date_separed(start_date)

        harvest_object_ids = []

        if harvester_type == 'ocn':
            for i in range(10):
                forecast_date = start_date + timedelta(days=i)
                fday, fmonth, fyear = self._format_date_separed(forecast_date)

                ftp_link = self._make_ftp_link(harvester_type, day, month,
                                               year, fday, fmonth, fyear)

                r_status_code = self._crawl_urls_ftp(ftp_link, 'cmems')

                if r_status_code == 226:
                    harvest_object_id = self._create_object(harvester_type,
                                                            ftp_link,
                                                            start_date,
                                                            forecast_date)
                    harvest_object_ids.append(harvest_object_id)

        else:
            ftp_link = self._make_ftp_link(harvester_type, day, month, year)

            r_status_code = self._crawl_urls_ftp(ftp_link, 'cmems')

            if r_status_code == 226:
                harvest_object_id = self._create_object(harvester_type,
                                                        ftp_link,
                                                        start_date,
                                                        None)
                harvest_object_ids.append(harvest_object_id)

        return harvest_object_ids

    def _make_ftp_link(self, harvester_type, day, month, year, fday=None,
                       fmonth=None, fyear=None):
        """
        Construct a link to a product based on the harvest type and start date.
        """

        if harvester_type == 'sst':
            return ("ftp://cmems.isac.cnr.it/Core/"
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

        elif harvester_type == 'sic_north':
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

        elif harvester_type == 'sic_south':
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

        elif harvester_type == 'ocn':
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

    def _product_end_date(self, product_start_date):
        return product_start_date + timedelta(days=1)

    def _product_enddate_url_parameter(self, start_date):
        return datetime.strftime(self._product_end_date(start_date), '%Y-%m-%d') + 'T00:00:00.000Z'  # noqa: E501

    def _format_date_separed(self, date):
        day = datetime.strftime(date, '%d')
        month = datetime.strftime(date, '%m')
        year = datetime.strftime(date, '%Y')

        return day, month, year

    def _get_metadata_create_objects(self, start_date, end_date,
                                     harvester_type):
        # Get contents
        try:
            url = "dummy"

            time_interval = end_date - start_date

            print(datetime.strftime(start_date, '%Y-%m-%d'))
            print('Start date' + str(start_date))

            base_start_date = start_date
            for idx in range(time_interval.days):
                start_date = base_start_date + timedelta(days=idx)
                print('idx = ' + str(idx))
                print('start_date = ' + str(start_date))
                id_list = self._get_products(harvester_type, start_date)

            return id_list

        except Exception as e:
            import traceback
            print(traceback.format_exc())
            self._save_gather_error('Unable to get content for URL: %s: %r'
                                    % (url, e), self.job)

            return None

    def _create_tags(self, harvester_type):
        """Create a list of tags based on the type of harvester."""
        tags_list = [{"name": "CMEMS"}]

        if harvester_type == 'sst':
            tags_list.extend([{"name": "SST"},
                              {"name": "sea surface temperature"},
                              {"name": "temperature"},
                              {"name": "sea"},
                              {"name": "observation"}])

        elif harvester_type == 'ocn':
            tags_list.extend([{"name": "arctic"},
                              {"name": "arctic ocean"},
                              {"name": "north"},
                              {"name": "northern"},
                              {"name": "forecast"},
                              {"name": "temperature"},
                              {"name": "salinity"},
                              {"name": "sea ice"},
                              {"name": "sea"},
                              {"name": "ice"},
                              {"name": "sea ice concentration"},
                              {"name": "sea ice thickness"},
                              {"name": "sea ice velocity"},
                              {"name": "sea ice type"}])

        else:
            tags_list.extend([{"name": "sea ice"},
                              {"name": "ice"},
                              {"name": "sea"},
                              {"name": "sea ice concentration"},
                              {"name": "observation"}])

            if harvester_type == 'sic_north':
                tags_list.extend([{"name": "north"},
                                  {"name": "northern"},
                                  {"name": "arctic"},
                                  {"name": "arctic ocean"}])

            elif harvester_type == 'sic_south':
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
        harvester_type = self.source_config['harvester_type']

        content = json.loads(content)
        ftp_link = content['ftp_link']
        start_date = deserialize_date(content['start_date'])
        day, month, year = self._format_date_separed(start_date)
        forecast_date = content.get('forecast_date')
        if forecast_date:
            forecast_date = deserialize_date(content['forecast_date'])
            fday, fmonth, fyear = self._format_date_separed(forecast_date)

        start_date = start_date.date()

        spatial_template = '{{"type":"Polygon", "coordinates":[{}]}}'
        metadata = {}

        if harvester_type == 'sst':
            metadata['identifier'] = ('SST-GLO-L4-DAILY-NRT-OBS-010-001-' +
                                      year +
                                      month +
                                      day)
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
            metadata['downloadLink'] = ftp_link
            metadata['thumbnail'] = ("http://cmems.isac.cnr.it/thredds/wms"
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
                                     str(start_date) +
                                     "T12:00:00.000Z")

        elif harvester_type == 'sic_north':
            metadata['identifier'] = ('SEAICE-CONC-NORTH-L4-DAILY-NRT-OBS-011-001-' +  # noqa E501
                                      year +
                                      month +
                                      day)
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
            metadata['downloadLinkEase'] = ftp_link
            metadata['downloadLinkPolstere'] = (
              "ftp://mftp.cmems.met.no/Core/"  # noqa: E121
              "SEAICE_GLO_SEAICE_L4_NRT_OBSERVATIONS_011_001/"
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
            metadata['thumbnail'] = ("http://thredds.met.no/thredds/wms/"
                                     "sea_ice/SIW-OSISAF-GLO-SIT_SIE_SIC-OBS/"
                                     "ice_conc_north_aggregated"
                                     "?request=GetMap"
                                     "&layers=ice_conc"
                                     "&version=1.3.0"
                                     "&crs=CRS:84"
                                     "&bbox=-180,0,180,90"
                                     "&WIDTH=800"
                                     "&HEIGHT=800"
                                     "&styles=boxfill/rainbow"
                                     "&format=image/png"
                                     "&time=" +
                                     str(start_date) +
                                     "T12:00:00.000Z")

        elif harvester_type == 'sic_south':
            metadata['identifier'] = ('SEAICE-CONC-SOUTH-L4-DAILY-NRT-OBS-011-001-' +  # noqa E501
                                      year +
                                      month +
                                      day)
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
            metadata['downloadLinkEase'] = ftp_link
            metadata['downloadLinkPolstere'] = (
                "ftp://mftp.cmems.met.no/Core/" +  # noqa: E121
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
            metadata['thumbnail'] = ("http://thredds.met.no/thredds/wms/"
                                     "sea_ice/SIW-OSISAF-GLO-SIT_SIE_SIC-OBS/"
                                     "ice_conc_south_aggregated"
                                     "?request=GetMap"
                                     "&layers=ice_conc"
                                     "&version=1.3.0"
                                     "&crs=CRS:84"
                                     "&bbox=-180,-90,180,0"
                                     "&WIDTH=800"
                                     "&HEIGHT=800"
                                     "&styles=boxfill/rainbow"
                                     "&format=image/png"
                                     "&time=" +
                                     str(start_date) +
                                     "T12:00:00.000Z")

        elif harvester_type == 'ocn':
            metadata['identifier'] = ('ARCTIC-FORECAST-' +
                                      fyear +
                                      fmonth +
                                      fday +
                                      '-PHYS-002-001-' +
                                      year +
                                      month +
                                      day)
            metadata['collection_id'] = 'ARCTIC_ANALYSIS_FORECAST_PHYS_002_001_A'  # noqa E501
            metadata['title'] = "Arctic Ocean Physics Analysis and Forecast"
            metadata['notes'] = ("Daily Arctic Ocean physics analysis to provide 10"  # noqa E501
                                 " days of forecast of the 3D physical ocean,"
                                 " including temperature, salinity, sea ice"
                                 " concentration, sea ice thickness, sea ice velocity"  # noqa E501
                                 " and sea ice type.")
            metadata['BulletinDate'] = str(start_date)
            metadata['ForecastDate'] = datetime.strftime(forecast_date,
                                                         '%Y-%m-%d')
            metadata['spatial'] = spatial_template.format([[-180, 90],
                                                           [180, 90],
                                                           [180, 63],
                                                           [-180, 63],
                                                           [-180, 90]])
            metadata['downloadLink'] = ftp_link
            metadata['thumbnail'] = ("http://thredds.met.no/thredds/wms/"  # noqa
                                     "topaz/"
                                     "dataset-topaz4-arc-1hr-myoceanv2-be"  # noqa
                                     "?request=GetMap"
                                     "&version=1.3.0"
                                     "&layers=temperature"
                                     "&CRS=CRS:84"
                                     "&bbox=-180,0,180,90"
                                     "&WIDTH=800"
                                     "&HEIGHT=800"
                                     "&styles=boxfill/rainbow"
                                     "&format=image/png"
                                     "&time=" +
                                     datetime.strftime(start_date,
                                        '%Y-%m-%d'))

        # Is there any way to determine the size of the downloads?
        # Would be good to have (or possibly required) in some cases, like
        # OpenSearch

        # Add common metadata
        metadata['name'] = metadata['identifier'].lower()
        metadata['StartTime'] = (str(start_date) + 'T00:00:00.000Z')
        metadata['StopTime'] = self._product_enddate_url_parameter(start_date)

        # For now, the collection name and description are the same as the
        # title and notes, though one or the other should probably change in
        # the future.
        metadata['collection_name'] = metadata['title']
        metadata['collection_description'] = metadata['notes']

        metadata['tags'] = self._create_tags(harvester_type)

        # Add time range metadata that's not tied to product-specific fields
        # like StartTime so that we can filter by a dataset's time range
        # without having to cram other kinds of temporal data into StartTime
        # and StopTime fields, etc. We do this for the Sentinel products.
        #
        # We'll want to revisit this later--it's still not clear if we can just
        # use StartTime and StopTime everywhere or if it has a special meaning
        # for certain kinds of products.
        metadata['timerange_start'] = metadata['StartTime']
        metadata['timerange_end'] = metadata['StopTime']

        return metadata

    # Required by NextGEOSS base harvester
    def _get_resources(self, metadata):
        """Return a list of resource dictionaries."""
        resources = []
        harvester_type = self.source_config['harvester_type']

        if harvester_type in {'sst', 'ocn'}:
            resources.append(self._make_resource(metadata['downloadLink'],
                                                 'Product Download'))

        else:
            resources.append(self._make_resource(metadata['downloadLinkEase'],
                                                 'Product Download (EASE GRID)'))  # noqa: E501
            resources.append(self._make_resource(metadata['downloadLinkPolstere'],  # noqa: E501
                                                 'Product Download (Polar Stereographic)'))  # noqa: E501

        resources.append(self._make_resource(metadata['thumbnail'],
                                             'Thumbnail Link'))

        return resources

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


def deserialize_date(date_string):
    """Deserialize dates serialized by calling str(date)."""
    return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
