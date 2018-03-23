# -*- coding: utf-8 -*-

import logging
import hashlib
import json
from datetime import timedelta
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

    def _get_metadata_create_objects(self,
                                     start_date,
                                     end_date,
                                     start_date_to_inc,
                                     harvest_job,
                                     current_guids,
                                     current_guids_in_harvest):
        # Get contents
        try:

            ids = []
            url = "dummy"

            year = str(start_date).split('-')[0]
            month = str(start_date).split('-')[1]
            day = str(start_date).split('-')[2]

            # the following string are almost equal, maybe the variables could
            # be defined without repeting code...
            sst_thumbnail = ("http://cmems.isac.cnr.it/thredds/wms"     #dif
                             "/METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2?"    #dif
                             "request=GetMap"
                             "&version=1.3.0"
                             "&layers=analysed_sst"     #dif
                             "&crs=CRS:84"
                             "&bbox=-180,-90,180,90"    #dif
                             "&WIDTH=800"
                             "&HEIGHT=800"
                             "&styles=boxfill/rainbow"
                             "&format=image/png"
                             "&time="
                             + str(start_date)
                             + 'T12:00:00.000Z')

            sic_north_thumbnail = ("http://thredds.met.no/thredds/wms/"    #dif
                                   "sea_ice/SIW-OSISAF-GLO-SIT_SIE_SIC-OBS/"  #dif
                                   "ice_conc_north_aggregated?"    #dif
                                   "request=GetMap"
                                   "&layers=ice_conc"   #dif
                                   "&version=1.3.0"
                                   "&crs=CRS:84"
                                   "&bbox=-180,0,180,90"    #dif
                                   "&WIDTH=800"
                                   "&HEIGHT=800"
                                   "&styles=boxfill/rainbow"
                                   "&format=image/png"
                                   "&time="
                                   + str(start_date)
                                   + "T12:00:00.000Z")

            sic_south_thumbnail = ("http://thredds.met.no/thredds/wms/"    #dif
                                   "sea_ice/SIW-OSISAF-GLO-SIT_SIE_SIC-OBS/"    #dif
                                   "ice_conc_south_aggregated?"    #dif
                                   "request=GetMap"
                                   "&layers=ice_conc"    #dif
                                   "&version=1.3.0"
                                   "&crs=CRS:84"
                                   "&bbox=-180,-90,180,0"    #dif
                                   "&WIDTH=800"
                                   "&HEIGHT=800"
                                   "&styles=boxfill/rainbow"
                                   "&format=image/png"
                                   "&time="
                                   + str(start_date)
                                   + "T12:00:00.000Z")

            if not hasattr(self, 'provider_logger'):
                self.provider_logger = self.make_provider_logger()

            for i in range(10):
                date_inc = start_date_to_inc + timedelta(days=i)
                start_date_inc = date_inc.date()
                ocn_forecast_thumbnail = ("http://thredds.met.no/thredds/wms/"
                                          "topaz/"
                                          "dataset-topaz4-arc-1hr-myoceanv2-be?"
                                          "request=GetMap"
                                          "&version=1.3.0"
                                          "&layers=temperature"
                                          "&CRS=CRS:84"
                                          "&bbox=-180,0,180,90"
                                          "&WIDTH=800"
                                          "&HEIGHT=800"
                                          "&styles=boxfill/rainbow"
                                          "&format=image/png"
                                          "&time="
                                          + str(start_date_inc))

                r_status_code = self._crawl_urls_simple(ocn_forecast_thumbnail,
                                                        'cmems')

                if r_status_code == 200:
                    metadata = {}
                    forecast = [str(start_date_inc), ocn_forecast_thumbnail]
                    fyear = forecast[0].split('-')[0]
                    fmonth = forecast[0].split('-')[1]
                    fday = forecast[0].split('-')[2]

                    metadata['datasetname'] = ('arctic-forecast-'
                                               + fyear
                                               + fmonth
                                               + fday
                                               + '-phys-002-001-'
                                               + year
                                               + month
                                               + day)

                    metadata['collection_id'] = 'ARCTIC_ANALYSIS_FORECAST_PHYS_002_001_A'
                    metadata['identifier'] = ('ARCTIC-FORECAST-'
                                              + fyear
                                              + fmonth
                                              + fday
                                              + '-PHYS-002-001-'
                                              + year
                                              + month
                                              + day)

                    metadata['StartTime'] = str(start_date) + 'T00:00:00.000Z'
                    metadata['StopTime'] = str(end_date) + 'T00:00:00.000Z'
                    metadata['BulletinDate'] = str(start_date)
                    metadata['ForecastDate'] = forecast[0]
                    metadata['Coordinates'] = [
                        [-180, 90],
                        [180, 90],
                        [180, 63],
                        [-180, 63],
                        [-180, 90]]

                    metadata['downloadLink'] = ("ftp://mftp.cmems.met.no/Core/"
                                                "ARCTIC_ANALYSIS_FORECAST_PHYS"
                                                "_002_001_a/"
                                                "dataset-topaz4-arc-myoceanv2-be/"
                                                + fyear
                                                + fmonth
                                                + fday
                                                + "_dm-metno-MODEL-topaz4-ARC-b"
                                                + year
                                                + month
                                                + day
                                                + "-fv02.0.nc")
                    metadata['thumbnail'] = forecast[1]

                    hash = hashlib.md5(json.dumps(metadata)).hexdigest()
                    if hash in current_guids:
                        current_guids_in_harvest.add(hash)
                    else:
                        obj = HarvestObject(job=harvest_job, guid=hash, extras=[
                            HOExtra(key='status',
                                    value='new'),
                            HOExtra(key='identifier',
                                    value=metadata['identifier']),
                            HOExtra(key='download_link',
                                    value=metadata['downloadLink']),
                            HOExtra(key='dataset_name',
                                    value=metadata['datasetname']),
                            # HOExtra(key='original_document',
                            # value=xml_string),
                            HOExtra(key='original_metadata',
                                    value=json.dumps(metadata)),
                            HOExtra(key='original_format',
                                    value='netCDF')
                        ])

                        obj.save()
                        ids.append(obj.id)

            cmems_collections = [sst_thumbnail,
                                 sic_north_thumbnail,
                                 sic_south_thumbnail]

            for collection in cmems_collections:
                r_status_code = self._crawl_urls_simple(collection, 'cmems')

                if r_status_code == 200:
                    metadata = {}

                    if collection == sst_thumbnail:
                        metadata['datasetname'] = ('sst-glo-l4'
                                                   '-daily-nrt-obs-010-001-'
                                                   + year
                                                   + month
                                                   + day)

                        metadata['collection_id'] = ('METOFFICE-GLO-SST-L4-NRT'
                                                     '-OBS-SST-V2')

                        metadata['identifier'] = ('SST-GLO-L4-DAILY-NRT-OBS'
                                                  '-010-001-'
                                                  + year
                                                  + month
                                                  + day)

                        metadata['StartTime'] = (str(start_date)
                                                 + 'T00:00:00.000Z')
                        metadata['StopTime'] = (str(end_date)
                                                + 'T00:00:00.000Z')
                        metadata['Coordinates'] = [[-180, 90],
                                                   [180, 90],
                                                   [180, -90],
                                                   [-180, -90],
                                                   [-180, 90]]
                        metadata['downloadLink'] = ("ftp://cmems.isac.cnr.it/"
                                                    "Core/"
                                                    "SST_GLO_SST_L4_NRT"
                                                    "_OBSERVATIONS_010_001/"
                                                    "METOFFICE-GLO-SST-L4-NRT"
                                                    "-OBS-SST-V2/"
                                                    + year
                                                    + "/"
                                                    + month
                                                    + "/"
                                                    + year
                                                    + month
                                                    + day
                                                    + "120000-UKMO-L4_GHRSST"
                                                    "-SSTfnd-OSTIA-GLOB-v02.0"
                                                    "-fv02.0.nc")
                        metadata['thumbnail'] = sst_thumbnail
                    elif collection == sic_north_thumbnail:
                        metadata['datasetname'] = ('seaice-conc-north-l4-daily'
                                                   '-nrt-obs-011-001-'
                                                   + year
                                                   + month
                                                   + day)
                        metadata['collection_id'] = ('METNO-GLO-SEAICE_CONC'
                                                     '-NORTH-L4-NRT-OBS')
                        metadata['identifier'] = ('SEAICE-CONC-NORTH-L4-DAILY'
                                                  '-NRT-OBS-011-001-'
                                                  + year
                                                  + month
                                                  + day)
                        metadata['STARTTIME'] = (str(start_date)
                                                 + 'T00:00:00.000Z')
                        metadata['StopTime'] = (str(end_date)
                                                + 'T00:00:00.000Z')
                        metadata['Coordinates'] = [[-180, 90],
                                                   [180, 90],
                                                   [180, 0],
                                                   [-180, 0],
                                                   [-180, 90]]
                        metadata['downloadLinkEase'] = ("ftp://mftp.cmems.met"
                                                        ".no/Core/SEAICE_GLO_"
                                                        "SEAICE_L4_NRT_"
                                                        "OBSERVATIONS_011_001/"
                                                        "METNO-GLO-SEAICE_CONC"
                                                        "-NORTH-L4-NRT-OBS/"
                                                        + year
                                                        + "/"
                                                        + month
                                                        + "/"
                                                        + "ice_conc_nh_ease-"
                                                        "125_multi_"
                                                        + year
                                                        + month
                                                        + day
                                                        + "1200.nc")
                        metadata['downloadLinkPolstere'] = ("ftp://mftp.cmems."
                                                            "met.no/Core/"
                                                            "SEAICE_GLO_"
                                                            "SEAICE_L4_NRT_"
                                                            "OBSERVATIONS_011_"
                                                            "001/METNO-GLO-SEA"
                                                            "ICE_CONC-NORTH-L4"
                                                            "-NRT-OBS/"
                                                            + year
                                                            + "/"
                                                            + month
                                                            + "/"
                                                            + "ice_conc_nh_"
                                                            "polstere-100_multi_"
                                                            + year
                                                            + month
                                                            + day
                                                            + "1200.nc")
                        metadata['thumbnail'] = sic_north_thumbnail
                    elif collection == sic_south_thumbnail:
                        metadata['datasetname'] = ('seaice-conc-south-l4-daily'
                                                   '-nrt-obs-011-001-'
                                                   + year
                                                   + month
                                                   + day)
                        metadata['collection_id'] = ('METNO-GLO-SEAICE_CONC'
                                                     '-SOUTH-L4-NRT-OBS')
                        metadata['identifier'] = ('SEAICE-CONC-SOUTH-L4-DAILY'
                                                  '-NRT-OBS-011-001-'
                                                  + year
                                                  + month
                                                  + day)
                        metadata['StartTime'] = (str(start_date)
                                                 + 'T00:00:00.000Z')
                        metadata['StopTime'] = (str(end_date)
                                                + 'T00:00:00.000Z')
                        metadata['Coordinates'] = [[-180, 0],
                                                   [180, 0],
                                                   [180, -90],
                                                   [-180, -90],
                                                   [-180, 0]]
                        metadata['downloadLinkEase'] = ("ftp://mftp.cmems.met"
                                                        ".no/Core/"
                                                        "SEAICE_GLO_SEAICE_L4_"
                                                        "NRT_OBSERVATIONS_011_"
                                                        "001/METNO-GLO-SEAICE_"
                                                        "CONC-SOUTH-L4-NRT-OBS/"
                                                        + year
                                                        + "/"
                                                        + month
                                                        + "/"
                                                        + "ice_conc_sh_ease-"
                                                        "125_multi_"
                                                        + year
                                                        + month
                                                        + day
                                                        + "1200.nc")
                        metadata['downloadLinkPolstere'] = ("ftp://mftp.cmems"
                                                            ".met.no/Core/"
                                                            "SEAICE_GLO_SEAICE"
                                                            "_L4_NRT_OBSERVATI"
                                                            "ONS_011_001/"
                                                            "METNO-GLO-SEAICE_"
                                                            "CONC-SOUTH-L4-NRT"
                                                            "-OBS/"
                                                            + year
                                                            + "/"
                                                            + month
                                                            + "/"
                                                            + "ice_conc_sh_"
                                                            "polstere-100_"
                                                            "multi_"
                                                            + year
                                                            + month
                                                            + day
                                                            + "1200.nc")
                        metadata['thumbnail'] = sic_south_thumbnail

                    # hash content data
                    hash = hashlib.md5(json.dumps(metadata)).hexdigest()
                    if hash in current_guids:
                        current_guids_in_harvest.add(hash)
                    else:

                        if collection == sst_thumbnail:
                            obj = HarvestObject(job=harvest_job,guid=hash, extras=[
                                HOExtra(key='status',
                                        value='new'),
                                HOExtra(key='identifier',
                                        value=metadata['identifier']),
                                HOExtra(key='download_link',
                                        value=metadata['downloadLink']),
                                HOExtra(key='dataset_name',
                                        value=metadata['datasetname']),
                                # HOExtra(key='original_document',
                                # value=xml_string),
                                HOExtra(key='original_metadata',
                                        value=json.dumps(metadata)),
                                HOExtra(key='original_format',
                                        value='netCDF')
                            ])
                        else:
                            obj = HarvestObject(job=harvest_job, guid=hash, extras=[
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
                                # HOExtra(key='original_document',
                                # value=xml_string),
                                HOExtra(key='original_metadata',
                                        value=json.dumps(metadata)),
                                HOExtra(key='original_format',
                                        value='netCDF')
                            ])

                        obj.save()
                        ids.append(obj.id)

            return ids

        except Exception as e:
            import traceback
            print traceback.format_exc()
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

        iso_values = metadata

        if 'tags' in iso_values:
            for tag in iso_values['tags']:
                tag = tag[:50] if len(tag) > 50 else tag
                tags.append({'name': tag})

        # Add default_tags from config
        default_tags = self.source_config.get('default_tags', [])
        if default_tags:
            for tag in default_tags:
                tags.append({'name': tag})

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

        extras = {
            'guid': harvest_object.guid,
            'spatial_harvester': True,
        }

        # Add default_extras from config
        default_extras = self.source_config.get('default_extras', {})
        if default_extras:
            override_extras = self.source_config.get('override_extras', False)
            for key, value in default_extras.iteritems():
                # log.debug('Processing extra %s', key)
                if not key in extras or override_extras:
                    # Look for replacement strings
                    if isinstance(value, basestring):
                        value = value.format(
                            harvest_source_id=harvest_object.job.source.id,
                            harvest_source_url=harvest_object.job.source.url.strip('/'),
                            harvest_source_title=harvest_object.job.source.title,
                            harvest_job_id=harvest_object.job.id)
                    extras[key] = value

        extras_as_dict = []
        for key, value in extras.iteritems():
            if isinstance(value, (list, dict)):
                extras_as_dict.append({'key': key, 'value': json.dumps(value)})
            else:
                extras_as_dict.append({'key': key, 'value': value})

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
