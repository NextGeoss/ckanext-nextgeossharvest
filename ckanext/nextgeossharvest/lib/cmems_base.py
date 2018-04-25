# -*- coding: utf-8 -*-

import logging
import hashlib
import json
from datetime import timedelta, datetime
import uuid

from ckan import model
from ckan import logic
from ckan.lib.navl.validators import not_empty
import ckan.plugins as plugins

from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObjectExtra as HOExtra
from ckanext.harvest.model import HarvestObject


log = logging.getLogger(__name__)


class CMEMSBase(HarvesterBase):

    def _create_object(self, metadata, harvester_type):

        hash = hashlib.md5(json.dumps(metadata)).hexdigest()

        extras = [HOExtra(key='status',
                          value='new'),
                  HOExtra(key='identifier',
                          value=metadata['identifier']),
                  HOExtra(key='dataset_name',
                          value=metadata['datasetname']),
                  HOExtra(key='original_metadata',
                          value=json.dumps(metadata)),
                  HOExtra(key='original_format',
                          value='netCDF'),
                  HOExtra(key='harvester_type',
                          value=harvester_type)]

        if harvester_type in {'sst', 'ocn'}:
            extras.append(HOExtra(key='download_link',
                                  value=metadata['downloadLink']))
        else:
            extras.extend([HOExtra(key='download_link_ease',
                                   value=metadata['downloadLinkEase']),
                           HOExtra(key='download_link_polstere',
                                   value=metadata['downloadLinkPolstere'])])

        obj = HarvestObject(job=self.job, guid=hash, extras=extras)

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
                    metadata = self._make_metadata(harvester_type, ftp_link,
                                                   start_date, day, month,
                                                   year, forecast_date,
                                                   fday, fmonth, fyear)
                    harvest_object_id = self._create_object(metadata,
                                                            harvester_type)
                    harvest_object_ids.append(harvest_object_id)

        else:
            ftp_link = self._make_ftp_link(harvester_type, day, month, year)

            r_status_code = self._crawl_urls_ftp(ftp_link, 'cmems')

            if r_status_code == 226:
                metadata = self._make_metadata(harvester_type, ftp_link,
                                               start_date, day, month, year)
                harvest_object_id = self._create_object(metadata,
                                                        harvester_type)
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

    def _make_metadata(self, harvester_type, ftp_link, start_date, day, month,
                       year, forecast_date=None, fday=None, fmonth=None,
                       fyear=None):
        """
        Create a metadata dictionary using the harvester type, link, and dates.
        """
        start_date = start_date.date()
        metadata = {}

        if harvester_type == 'sst':
            metadata['identifier'] = ('SST-GLO-L4-DAILY-NRT-OBS-010-001-' +
                                      year +
                                      month +
                                      day)
            metadata['collection_id'] = ('METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2')
            metadata['Coordinates'] = [[-180, 90],
                                       [180, 90],
                                       [180, -90],
                                       [-180, -90],
                                       [-180, 90]]
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
            metadata['Coordinates'] = [[-180, 90],
                                       [180, 90],
                                       [180, 0],
                                       [-180, 0],
                                       [-180, 90]]
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
            metadata['Coordinates'] = [[-180, 0],
                                       [180, 0],
                                       [180, -90],
                                       [-180, -90],
                                       [-180, 0]]
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
            metadata['BulletinDate'] = str(start_date)
            metadata['ForecastDate'] = datetime.strftime(forecast_date,
                                                         '%Y-%m-%d')
            metadata['Coordinates'] = [
                [-180, 90],
                [180, 90],
                [180, 63],
                [-180, 63],
                [-180, 90]]
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

        # Add common metadata
        metadata['datasetname'] = metadata['identifier'].lower()
        metadata['StartTime'] = (str(start_date) + 'T00:00:00.000Z')
        metadata['StopTime'] = self._product_enddate_url_parameter(start_date)

        return metadata

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

            year, month, day = str(start_date).split('-')
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

    def _create_package_dict(self, harvest_object, context):

        original_metadata = self._get_object_extra(harvest_object,
                                                   'original_metadata')

        # Parse document

        metadata = json.loads(original_metadata)

        # Build the package dict

        dataset_name = self._get_object_extra(harvest_object, 'dataset_name')

        spatial_json = ('{"type":"Polygon",'
                        '"crs":'
                        '{"type":"EPSG",'
                        '"properties":'
                        '{"code":4326,'
                        '"coordinate_order":"Long,Lat"}},'
                        '"coordinates":[' + str(metadata['Coordinates']) +
                        ']}')

        extras_dict = (self
                       ._generateExtrasDict(
                           name=dataset_name
                           .upper(),
                           metadata=metadata,
                           spatial=spatial_json))

        tags_list = self._create_tags(harvest_object)

        # ##### FINISHED ADD EXTRAS
        # Build the package dict
        package_dict = (self
                        ._get_package_dict(
                            metadata,
                            harvest_object,
                            extras_dict,
                            tags_list))

        if not package_dict:
            log.error(('No package dict returned,'
                       ' aborting import for object {0}')
                      .format(harvest_object.id))
            return False

        # Create / update the package
        context.update({
                       'extras_as_string': True,
                       'api_version': '2',
                       'return_id_only': True})

        if self._site_user and context['user'] == self._site_user['name']:
            context['ignore_auth'] = True

        # The default package schema does not like Upper case tags
        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty, unicode]

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        self._create_dataset(
            harvest_object,
            context,
            package_dict,
            tag_schema,
            metadata)

    def _create_dataset(self,
                        harvest_object,
                        context,
                        package_dict,
                        tag_schema,
                        metadata):

        package_schema = logic.schema.default_create_package_schema()
        package_schema['tags'] = tag_schema
        context['schema'] = package_schema

        # We need to explicitly provide a package ID,
        # otherwise ckanext-spatial
        # won't be be able to link the extent to the package.
        package_dict['id'] = unicode(uuid.uuid4())
        package_schema['id'] = [unicode]

        # Save reference to the package on the object
        harvest_object.package_id = package_dict['id']
        harvest_object.add()
        # Defer constraints and flush so the dataset can be indexed with
        # the harvest object id (on the after_show hook from the harvester
        # plugin)
        model.Session.execute('SET CONSTRAINTS '
                              'harvest_object_package_id_fkey DEFERRED')
        model.Session.flush()

        try:
            package_id = (plugins
                          .toolkit
                          .get_action('package_create')
                          (context, package_dict))
            log.info('Created new package %s with guid %s',
                     package_id,
                     harvest_object.guid)

            for key, value in metadata.iteritems():
                # create resources

                if key == 'downloadLink':
                    resource_dict = {}
                    resource_dict['package_id'] = package_id
                    resource_dict['url'] = str(value)
                    resource_dict['name'] = 'Product Download'
                    resource_dict['description'] = ('Download the netCDF'
                                                    ' from CMEMS. NOTE:'
                                                    ' DOWNLOAD REQUIRES'
                                                    ' LOGIN')
                    resource_dict['format'] = 'netcdf'
                    resource_dict['mimetype'] = 'application/x-netcdf'
                    (plugins.toolkit
                     .get_action('resource_create')
                     (context, resource_dict))

                if key == 'downloadLinkEase':
                    resource_dict = {}
                    resource_dict['package_id'] = package_id
                    resource_dict['url'] = str(value)
                    resource_dict['name'] = 'Product Download (EASE GRID)'
                    resource_dict['description'] = ('Download the netCDF'
                                                    ' from CMEMS. NOTE:'
                                                    ' DOWNLOAD REQUIRES'
                                                    ' LOGIN')
                    resource_dict['format'] = 'netcdf'
                    resource_dict['mimetype'] = 'application/x-netcdf'
                    (plugins.toolkit
                     .get_action('resource_create')
                     (context, resource_dict))

                if key == 'downloadLinkPolstere':
                    resource_dict = {}
                    resource_dict['package_id'] = package_id
                    resource_dict['url'] = str(value)
                    resource_dict['name'] = ('Product Download'
                                             ' (Polar Stereographic)')
                    resource_dict['description'] = ('Download the netCDF'
                                                    ' from CMEMS. NOTE:'
                                                    ' DOWNLOAD REQUIRES'
                                                    ' LOGIN')
                    resource_dict['format'] = 'netcdf'
                    resource_dict['mimetype'] = 'application/x-netcdf'
                    (plugins.toolkit
                     .get_action('resource_create')
                     (context, resource_dict))

                if key == 'thumbnail':
                    resource_dict = {}
                    resource_dict['package_id'] = package_id
                    resource_dict['url'] = value
                    resource_dict['name'] = 'Thumbnail Link'
                    resource_dict['format'] = 'png'
                    resource_dict['mimetype'] = 'image/png'
                    (plugins.toolkit
                     .get_action('resource_create')(context,
                                                    resource_dict))

        except plugins.toolkit.ValidationError as e:
            self._save_object_error(
                'Validation Error: %s'
                % str(e.error_summary),
                harvest_object,
                'Import')
            return False
        except Exception as e:
            self._save_object_error('Error: %s'
                                    % str(e),
                                    harvest_object,
                                    'Import')
            return False

    def _create_tags(self, harvest_object):
        """Create a list of tags based on the type of harvester."""
        harvester_type = self._get_object_extra(harvest_object,
                                                'harvester_type')

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

    def _get_package_dict(self,
                          metadata,
                          harvest_object,
                          extras_dict,
                          tags_dict):
        '''
        Constructs a package_dict suitable to be passed to package_create or
        package_update. See documentation on
        ckan.logic.action.create.package_create for more details
        Extensions willing to modify the dict should do so implementing the
        ISpatialHarvester interface
            import ckan.plugins as p
            from ckanext.spatial.interfaces import ISpatialHarvester
            class MyHarvester(p.SingletonPlugin):
                p.implements(ISpatialHarvester, inherit=True)
                def get_package_dict(self, context, data_dict):
                    package_dict = data_dict['package_dict']
                    package_dict['extras'].append(
                        {'key': 'my-custom-extra', 'value': 'my-custom-value'}
                    )
                    return package_dict
        If a dict is not returned by this function, the import stage
        will be cancelled.
        :param iso_values: Dictionary with parsed values from the ISO 19139
            XML document
        :type iso_values: dict
        :param harvest_object: HarvestObject domain object (with access to
            job and source objects)
        :type harvest_object: HarvestObject
        :returns: A dataset dictionary (package_dict)
        :rtype: dict
        '''

        tags = tags_dict
        for tag in tags:
            if tag.get('name') == "SST":
                dataset_name = "Global Observed Sea Surface Temperature"
                notes = ("Daily analysis of sea surface temperature (SST),"
                         " based on measurements from several satellite and"
                         " in situ SST datasets, for the global ocean and"
                         " some lakes.")
            elif tag.get('name') == "observation":
                for tag2 in tags:
                    if tag2.get('name') == "antarctic":
                        dataset_name = ("Antarctic Ocean Observed"
                                        " Sea Ice Concentration")
                        notes = ("Daily sea ice concentration at 10km "
                                 "resolution in polar stereographic and EASE"
                                 " grid projections covering the Southern"
                                 " Hemisphere.")
                    elif tag2.get('name') == "arctic":
                        dataset_name = ("Arctic Ocean Observed Sea Ice"
                                        " Concentration")
                        notes = ("Daily sea ice concentration at 10km"
                                 " resolution in polar stereographic and EASE"
                                 " grid projections covering the Northern"
                                 " Hemisphere.")
            elif tag.get('name') == "forecast":
                dataset_name = "Arctic Ocean Physics Analysis and Forecast"
                notes = ("Daily Arctic Ocean physics analysis to provide 10"
                         " days of forecast of the 3D physical ocean,"
                         " including temperature, salinity, sea ice"
                         " concentration, sea ice thickness, sea ice velocity"
                         " and sea ice type.")

        package_dict = {
            'title': dataset_name,
            'notes': notes,
            'tags': tags,  # overriding previous operations
            'resources': [],
            'extras': extras_dict,
        }

        # We need to get the owner organization (if any) from the harvest

        source_dataset = model.Package.get(harvest_object.source.id)
        if source_dataset.owner_org:
            package_dict['owner_org'] = source_dataset.owner_org

        package_dict['name'] = metadata['datasetname']

        return package_dict

    def _generateExtrasDict(self, name, metadata, **kwargs):
        extras_dict = []

        for a in kwargs:
            extras_dict += [{"value": kwargs[a], "key": a}]

        for key, value in metadata.iteritems():
            if key not in {'Coordinates', 'metadataLink', 'downloadLink',
                           'spatial'}:
                extras_dict += [{"value": value, "key": key}]
        return extras_dict
