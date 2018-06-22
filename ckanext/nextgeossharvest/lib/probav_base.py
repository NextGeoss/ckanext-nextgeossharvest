import json
from ckan.plugins.core import SingletonPlugin
from ckanext.spatial.harvesters.base import SpatialHarvester
from ckan import model


class PROBAVBase(SpatialHarvester, SingletonPlugin):
    def get_package_dict(self, metadata, harvest_object, extras_dict,
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
                        {'key': 'my-custom-extra', 'value': 'my-custom-value'}  # noqa: E501
                    )
                    return package_dict
        If a dict is not returned by this function, the import stage will be cancelled.  # noqa: E501
        :param iso_values: Dictionary with parsed values from the ISO 19139
            XML document
        :type iso_values: dict
        :param harvest_object: HarvestObject domain object (with access to
            job and source objects)
        :type harvest_object: HarvestObject
        :returns: A dataset dictionary (package_dict)
        :rtype: dict
        '''
        # log.info(tags)

        tags = tags_dict
        for tag in tags:
            if tag.get('name') == "S1-TOC":
                dataset_name = "Proba-V S1-TOC"
            elif tag.get('name') == "S1-TOA":
                dataset_name = "Proba-V S1-TOA"
            elif tag.get('name') == "S10-TOC":
                dataset_name = "Proba-V S10-TOC"
            elif tag.get('name') == "S5-TOC":
                dataset_name = "Proba-V S5-TOC"
            elif tag.get('name') == "S5-TOA":
                dataset_name = "Proba-V S5-TOA"
            elif tag.get('name') == "L2A":
                dataset_name = "Proba-V Level-2A"
            elif tag.get('name') == "L1C":
                dataset_name = "Proba-V Level-1C"

        for tag in tags:
            if tag.get('name') == "NDVI":
                dataset_name = dataset_name + " NDVI"

        for tag in tags:
            if tag.get('name') == "1KM":
                dataset_name = dataset_name + " (1KM)"
            elif tag.get('name') == "333M":
                dataset_name = dataset_name + " (333M)"
            elif tag.get('name') == "100M":
                dataset_name = dataset_name + " (100M)"

        if dataset_name == "Proba-V S1-TOC (1KM)":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of 1 day for 1Km of spatial resolution."  # noqa: E501

        elif dataset_name == "Proba-V S1-TOA (1KM)":
            notes = "Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame of 1 day for 1Km of spatial resolution."  # noqa: E501

        elif dataset_name == "Proba-V S10-TOC (1KM)":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of 10 days for 1Km of spatial resolution."  # noqa: E501

        elif dataset_name == "Proba-V S10-TOC NDVI (1KM)":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of 10 days for 1Km of spatial resolution, containing only Normalized Difference Vegetation Index (NDVI)."  # noqa: E501

        elif dataset_name == "Proba-V Level-2A (1KM)":
            notes = "PROBA-V Level2A - 1KM segments contain the Level 1C (P product) data projected on a uniform 1Km grid."  # noqa: E501

        elif dataset_name == "Proba-V Level-1C":
            notes = "Raw data which is geo-located and radiometrically calibrated to Top Of Atmosphere (TOA) reflectance values."  # noqa: E501

        elif dataset_name == "Proba-V S1-TOC (333M)":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of 1 day for 333m of spatial resolution."  # noqa: E501

        elif dataset_name == "Proba-V S1-TOA (333M)":
            notes = "Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame of 1 day for 333m of spatial resolution."  # noqa: E501

        elif dataset_name == "Proba-V S10-TOC (333M)":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of 10 days for 333m of spatial resolution."  # noqa: E501

        elif dataset_name == "Proba-V S10-TOC NDVI (333M)":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of 10 days for 333m of spatial resolution, containing only Normalized Difference Vegetation Index (NDVI)."  # noqa: E501

        elif dataset_name == "Proba-V Level-2A (333M)":
            notes = "PROBA-V Level2A - 333M segments contain the Level 1C (P product) data projected on a uniform 333m grid."  # noqa: E501

        elif dataset_name == "Proba-V S1-TOC (100M)":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of 1 day for 100m of spatial resolution."  # noqa: E501

        elif dataset_name == "Proba-V S1-TOA (100M)":
            notes = "Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame of 1 day for 100m of spatial resolution."  # noqa: E501

        elif dataset_name == "Proba-V S1-TOC NDVI (100M)":
            notes = "Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame of 1 day for 100m of spatial resolution, containing only Normalized Difference Vegetation Index (NDVI)."  # noqa: E501

        elif dataset_name == "Proba-V S5-TOC (100M)":
            notes = "Synthesis products with Top of Canopy (TOC) reflectances composited over defined time frame of 5 days for 100m of spatial resolution."  # noqa: E501

        elif dataset_name == "Proba-V S5-TOA (100M)":
            notes = "Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame of 5 days for 100m of spatial resolution."  # noqa: E501

        elif dataset_name == "Proba-V S5-TOC NDVI (100M)":
            notes = "Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame of 5 days for 100m of spatial resolution, containing only Normalized Difference Vegetation Index (NDVI)."  # noqa: E501

        elif dataset_name == "Proba-V Level-2A (100M)":
            notes = "PROBA-V Level2A - 100M segments contain the Level 1C (P product) data projected on a uniform 100m grid."  # noqa: E501

        uuid = self._get_object_extra(harvest_object, 'uuid')

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
        # source dataset
        source_dataset = model.Package.get(harvest_object.source.id)
        if source_dataset.owner_org:
            package_dict['owner_org'] = source_dataset.owner_org

        # Package name
        package = harvest_object.package
        if package is None or package.title != dataset_name:
            name = self._gen_new_name(dataset_name)
            if not name:
                name = self._gen_new_name(str(uuid))
            if not name:
                raise Exception(
                    'Could not generate a unique name from the title or the GUID. Please choose a more unique title.'  # noqa: E501
                )
            package_dict['name'] = name
        else:
            package_dict['name'] = package.name

        extras = {
            'guid': harvest_object.guid,
            'spatial_harvester': True,
        }

        # Add default_extras from config
        default_extras = self.source_config.get('default_extras', {})
        if default_extras:
            override_extras = self.source_config.get('override_extras', False)  # noqa: E501
            for key, value in default_extras.iteritems():
                #log.debug('Processing extra %s', key)
                if not key in extras or override_extras:
                    # Look for replacement strings
                    if isinstance(value, basestring):
                        value = value.format(
                            harvest_source_id=harvest_object.job.source.id,
                            harvest_source_url=harvest_object.job.source.url.
                            strip('/'),
                            harvest_source_title=harvest_object.job.source.
                            title,
                            harvest_job_id=harvest_object.job.id,
                            harvest_object_id=harvest_object.id)
                    extras[key] = value

        extras_as_dict = []
        for key, value in extras.iteritems():
            if isinstance(value, (list, dict)):
                extras_as_dict.append({'key': key, 'value': json.dumps(value)})  # noqa: E501
            else:
                extras_as_dict.append({'key': key, 'value': value})

        return package_dict

    def checkIfCoordsAreCircular(self, coords):
        # check if first and last coords are the same, if not, add first coord to last pos  # noqa: E501
        if coords[0] == coords[len(coords) - 1]:
            return coords
        else:
            coords.append(coords[0])
            return coords

    def createStringCoords(self, coords):
        coords_string = "["

        for c in range(0, len(coords)):
            coords_string += "["+str(coords[c][0]) + \
                ","+str(coords[c][1])+"]"  # long, lat
            if c + 1 != len(coords):
                coords_string += ','

        coords_string += ']'
        return coords_string

    def generateExtrasDict(self, name, metadata, **kwargs):
        extras_dict = []

        for a in kwargs:
            extras_dict += [{"value": kwargs[a], "key": a}]

        for key, value in metadata.iteritems():
            if key != 'Coordinates' and key != 'metadataLink' and key != 'downloadLink' and key != 'thumbnail' and key != 'spatial':  # noqa: E501
                extras_dict += [{"value": value, "key": key}]
        return extras_dict
