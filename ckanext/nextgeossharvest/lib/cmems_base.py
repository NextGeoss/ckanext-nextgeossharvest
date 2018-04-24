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

    def _create_object(self, id_list, metadata, collection_flag):

        hash = hashlib.md5(json.dumps(metadata)).hexdigest()

        if hash in self.current_guids:
            self.current_guids_in_harvest.add(hash)
        else:
            if collection_flag:
                obj = HarvestObject(job=self.harvest_job, guid=hash, extras=[
                    HOExtra(key='status',
                            value='new'),
                    HOExtra(key='identifier',
                            value=metadata['identifier']),
                    HOExtra(key='download_link',
                            value=metadata['downloadLink']),
                    HOExtra(key='dataset_name',
                            value=metadata['datasetname']),
                    HOExtra(key='original_metadata',
                            value=json.dumps(metadata)),
                    HOExtra(key='original_format',
                            value='netCDF')
                ])
            else:
                obj = HarvestObject(job=self.harvest_job, guid=hash, extras=[
                    HOExtra(key='status',
                            value='new'),
                    HOExtra(key='identifier',
                            value=metadata['identifier']),
                    HOExtra(key='download_link_ease',
                            value=metadata['downloadLinkEase']),
                    HOExtra(key='download_link_polstere',
                            value=metadata['downloadLinkPolstere']),
                    HOExtra(key='dataset_name',
                            value=metadata['datasetname']),
                    HOExtra(key='original_metadata',
                            value=json.dumps(metadata)),
                    HOExtra(key='original_format',
                            value='netCDF')
                ])

            obj.save()
            id_list.append(obj.id)

    def _get_sst_product(self, id_list, metadata, start_date):
        day, month, year = self._format_date_separed(start_date)

        sst_ftp_link = ("ftp://cmems.isac.cnr.it/Core/"
                        "SST_GLO_SST_L4_NRT_OBSERVATIONS_010_001/"
                        "METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2/" +
                        year +
                        "/" +
                        month +
                        "/" +
                        year +
                        month +
                        day +
                        "120000-UKMO-L4_GHRSST-SSTfnd-OSTIA-GLOB-v02.0-fv02.0.nc")  # noqa E501

        r_status_code = self._crawl_urls_ftp(sst_ftp_link, 'cmems')

        if r_status_code == 226:
            start_date = start_date.date()
            metadata['datasetname'] = ('sst-glo-l4-daily-nrt-obs-010-001-' +
                                       year +
                                       month +
                                       day)

            metadata['collection_id'] = ('METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2')

            metadata['identifier'] = ('SST-GLO-L4-DAILY-NRT-OBS-010-001-' +
                                      year +
                                      month +
                                      day)

            metadata['StartTime'] = (str(start_date) + 'T00:00:00.000Z')

            metadata['StopTime'] = self._product_enddate_url_parameter(start_date)  # noqa: E501

            metadata['Coordinates'] = [[-180, 90],
                                       [180, 90],
                                       [180, -90],
                                       [-180, -90],
                                       [-180, 90]]

            metadata['downloadLink'] = sst_ftp_link

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

            self._create_object(id_list, metadata, True)

    def _get_sic_north_product(self, id_list, metadata, start_date):
        day, month, year = self._format_date_separed(start_date)

        sic_north_ftp_link = ("ftp://mftp.cmems.met.no/Core/"
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

        r_status_code = self._crawl_urls_ftp(sic_north_ftp_link, 'cmems')

        if r_status_code == 226:
            start_date = start_date.date()
            metadata['datasetname'] = ('seaice-conc-north-l4-daily'
                                       '-nrt-obs-011-001-' +
                                       year +
                                       month +
                                       day)

            metadata['collection_id'] = ('METNO-GLO-SEAICE_CONC-NORTH-L4-NRT-OBS')  # noqa E501

            metadata['identifier'] = ('SEAICE-CONC-NORTH-L4-DAILY-NRT-OBS-011-001-' +  # noqa E501
                                      year +
                                      month +
                                      day)

            metadata['StartTime'] = (str(start_date) + 'T00:00:00.000Z')

            metadata['StopTime'] = self._product_enddate_url_parameter(start_date)  # noqa: E501

            metadata['Coordinates'] = [[-180, 90],
                                       [180, 90],
                                       [180, 0],
                                       [-180, 0],
                                       [-180, 90]]

            metadata['downloadLinkEase'] = sic_north_ftp_link

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

            self._create_object(id_list, metadata, False)

    def _get_sic_south_product(self, id_list, metadata, start_date):
        day, month, year = self._format_date_separed(start_date)

        sic_south_ftp_link = ("ftp://mftp.cmems.met.no/Core/"
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

        r_status_code = self._crawl_urls_ftp(sic_south_ftp_link, 'cmems')

        if r_status_code == 226:

            start_date = start_date.date()

            metadata['datasetname'] = ('seaice-conc-south-l4-daily'
                                       '-nrt-obs-011-001-' +
                                       year +
                                       month +
                                       day)

            metadata['collection_id'] = ('METNO-GLO-SEAICE_CONC-SOUTH-L4-NRT-OBS')  # noqa E501

            metadata['identifier'] = ('SEAICE-CONC-SOUTH-L4-DAILY-NRT-OBS-011-001-' +  # noqa E501
                                      year +
                                      month +
                                      day)

            metadata['StartTime'] = str(start_date) + 'T00:00:00.000Z'

            metadata['StopTime'] = self._product_enddate_url_parameter(start_date)  # noqa: E501

            metadata['Coordinates'] = [[-180, 0],
                                       [180, 0],
                                       [180, -90],
                                       [-180, -90],
                                       [-180, 0]]

            metadata['downloadLinkEase'] = sic_south_ftp_link

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

            self._create_object(id_list, metadata, False)

    def _get_ocn_forecast_products(self, id_list, metadata, start_date):
        day, month, year = self._format_date_separed(start_date)
        start_date = start_date.date()

        for i in range(10):

            forecast_date = start_date + timedelta(days=i)
            fday, fmonth, fyear = self._format_date_separed(forecast_date)

            ocn_forecast_link = ("ftp://mftp.cmems.met.no/Core/"
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

            r_status_code = self._crawl_urls_ftp(ocn_forecast_link, 'cmems')

            if r_status_code == 226:

                metadata['datasetname'] = ('arctic-forecast-' +
                                           fyear +
                                           fmonth +
                                           fday +
                                           '-phys-002-001-' +
                                           year +
                                           month +
                                           day)

                metadata['collection_id'] = 'ARCTIC_ANALYSIS_FORECAST_PHYS_002_001_A'  # noqa E501

                metadata['identifier'] = ('ARCTIC-FORECAST-' +
                                          fyear +
                                          fmonth +
                                          fday +
                                          '-PHYS-002-001-' +
                                          year +
                                          month +
                                          day)

                metadata['StartTime'] = str(start_date) + 'T00:00:00.000Z'

                metadata['StopTime'] = self._product_enddate_url_parameter(start_date)  # noqa: E501

                metadata['BulletinDate'] = str(start_date)

                metadata['ForecastDate'] = datetime.strftime(forecast_date,
                                                             '%Y-%m-%d')
                metadata['Coordinates'] = [
                    [-180, 90],
                    [180, 90],
                    [180, 63],
                    [-180, 63],
                    [-180, 90]]

                metadata['downloadLink'] = ocn_forecast_link

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

                self._create_object(id_list, metadata, True)

    def _product_end_date(self, product_start_date):
        return product_start_date + timedelta(days=1)

    def _product_enddate_url_parameter(self, start_date):
        return datetime.strftime(self._product_end_date(start_date), '%Y-%m-%d') + 'T00:00:00.000Z'  # noqa: E501

    def _format_date_separed(self, date):
        day = datetime.strftime(date, '%d')
        month = datetime.strftime(date, '%m')
        year = datetime.strftime(date, '%Y')

        return day, month, year

    def _get_metadata_create_objects(self,
                                     start_date,
                                     end_date,
                                     harvest_job,
                                     current_guids,
                                     current_guids_in_harvest,
                                     harvester_type):
        # Get contents
        try:
            metadata_dict = dict()
            id_list = list()
            url = "dummy"

            year, month, day = str(start_date).split('-')
            time_interval = end_date - start_date

            self.harvest_job = harvest_job
            self.current_guids = current_guids
            self.current_guids_in_harvest = current_guids_in_harvest

            print(datetime.strftime(start_date, '%Y-%m-%d'))
            print('Start date' + str(start_date))

            base_start_date = start_date
            for idx in range(time_interval.days):
                start_date = base_start_date + timedelta(days=idx)
                print('idx = ' + str(idx))
                print('start_date = ' + str(start_date))
                if harvester_type == 'sst':
                    self._get_sst_product(id_list, metadata_dict, start_date)
                elif harvester_type == 'sic_north':
                    self._get_sic_north_product(id_list, metadata_dict,
                                                start_date)
                elif harvester_type == 'sic_south':
                    self._get_sic_south_product(id_list, metadata_dict,
                                                start_date)
                elif harvester_type == 'ocn':
                    self._get_ocn_forecast_products(id_list, metadata_dict,
                                                    start_date)
            return id_list
        except Exception as e:
            import traceback
            print(traceback.format_exc())
            self._save_gather_error('Unable to get content for URL: %s: %r'
                                    % (url, e), harvest_job)
            return None

    def _delete_dataset(self, harvest_object, context):
        log = logging.getLogger(__name__ + '.import')
        context.update({
            'ignore_auth': True,
        })

        plugins.toolkit.get_action('package_delete')(
            context, {'id': harvest_object.package_id})

        log.info('Deleted package {0} with guid {1}'.format(
            harvest_object.package_id, harvest_object.guid))

    def _create_package_dict(self, harvest_object, context, previous_object):

        original_metadata = self._get_object_extra(harvest_object,
                                                   'original_metadata')

        # Parse document

        metadata = json.loads(original_metadata)
        uuid = self._get_object_extra(harvest_object, 'uuid')

        # Flag previous object as not current anymore

        if previous_object and not self.force_import:
            previous_object.current = False
            previous_object.add()

        # Update GUID with the one on the document
        iso_guid = uuid    # iso_values['guid']
        if iso_guid and harvest_object.guid != iso_guid:
            # First make sure there already aren't current objects
            # with the same guid
            existing_object = (model.Session.query(HarvestObject.id)
                               .filter(HarvestObject.guid == iso_guid)
                               .filter(HarvestObject.current is True)
                               .first())
            if existing_object:
                self._save_object_error('Object {0} already has this guid {1}'
                                        .format(existing_object.id, iso_guid),
                                        harvest_object, 'Import')
                return False

            harvest_object.guid = iso_guid
            harvest_object.add()

        # Generate GUID if not present (i.e. it's a manual import)
        if not harvest_object.guid:
            m = hashlib.md5()
            m.update(harvest_object.content.encode('utf8', 'ignore'))
            harvest_object.guid = m.hexdigest()
            harvest_object.add()

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

        try:
            thumbnail = metadata['thumbnail']
            extras_dict = (self
                           ._generateExtrasDict(
                               name=dataset_name
                               .upper(),
                               metadata=metadata,
                               thumbnail=thumbnail,
                               spatial=spatial_json))
        except Exception:
            extras_dict = (self
                           ._generateExtrasDict(
                               name=dataset_name.upper(),
                               metadata=metadata,
                               spatial=spatial_json))

        tags_dict = [{"name": "CMEMS"}]

        if 'sst' in dataset_name.lower():
            tags_dict.append({"name": "SST"})
            tags_dict.append({"name": "sea surface temperature"})
            tags_dict.append({"name": "temperature"})
            tags_dict.append({"name": "sea"})
            tags_dict.append({"name": "observation"})

        elif 'seaice' in dataset_name.lower():
            tags_dict.append({"name": "sea ice"})
            tags_dict.append({"name": "ice"})
            tags_dict.append({"name": "sea"})
            tags_dict.append({"name": "sea ice concentration"})
            tags_dict.append({"name": "observation"})

            if 'north' in dataset_name.lower():
                tags_dict.append({"name": "north"})
                tags_dict.append({"name": "northern"})
                tags_dict.append({"name": "arctic"})
                tags_dict.append({"name": "arctic ocean"})

            elif 'south' in dataset_name.lower():
                tags_dict.append({"name": "south"})
                tags_dict.append({"name": "Southern"})
                tags_dict.append({"name": "antarctic"})
                tags_dict.append({"name": "antarctic ocean"})

        elif 'arctic' in dataset_name.lower():
            tags_dict.append({"name": "arctic"})
            tags_dict.append({"name": "arctic ocean"})
            tags_dict.append({"name": "south"})
            tags_dict.append({"name": "southern"})
            tags_dict.append({"name": "forecast"})
            tags_dict.append({"name": "temperature"})
            tags_dict.append({"name": "salinity"})
            tags_dict.append({"name": "sea ice"})
            tags_dict.append({"name": "sea"})
            tags_dict.append({"name": "ice"})
            tags_dict.append({"name": "sea ice concentration"})
            tags_dict.append({"name": "sea ice thickness"})
            tags_dict.append({"name": "sea ice velocity"})
            tags_dict.append({"name": "sea ice type"})

        # ##### FINISHED ADD EXTRAS
        # Build the package dict
        package_dict = (self
                        ._get_package_dict(
                            metadata,
                            harvest_object,
                            extras_dict,
                            tags_dict))

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
            previous_object,
            context,
            package_dict,
            tag_schema,
            metadata)

    def _create_dataset(self,
                        harvest_object,
                        previous_object,
                        context,
                        package_dict,
                        tag_schema,
                        metadata):

        status = self._get_object_extra(harvest_object, 'status')

        if status == 'new':
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

        elif status == 'change':

            # Check if the modified date is more recent
            if (not self.force_import and
                    previous_object and
                    harvest_object.metadata_modified_date <=
                    previous_object.metadata_modified_date):
                # Assign the previous job id to the new object to
                # avoid losing history
                harvest_object.harvest_job_id = previous_object.job.id
                harvest_object.add()

                # Delete the previous object
                # to avoid cluttering the object table
                previous_object.delete()

                log.info('Document with GUID %s unchanged, skipping...'
                         % (harvest_object.guid))
            else:
                package_schema = logic.schema.default_update_package_schema()
                package_schema['tags'] = tag_schema
                context['schema'] = package_schema

                package_dict['id'] = harvest_object.package_id
                try:
                    package_id = (plugins.toolkit
                                  .get_action('package_update')
                                  (context, package_dict))
                    log.info('Updated package %s with guid %s',
                             package_id,
                             harvest_object.guid)
                except plugins.toolkit.ValidationError, e:
                    self._save_object_error('Validation Error: %s'
                                            % str(e.error_summary),
                                            harvest_object, 'Import')
                    return False

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
            if (
                    key != 'Coordinates' and
                    key != 'metadataLink' and
                    key != 'downloadLink' and
                    key != 'thumbnail' and
                    key != 'spatial'):
                extras_dict += [{"value": value, "key": key}]
        return extras_dict
