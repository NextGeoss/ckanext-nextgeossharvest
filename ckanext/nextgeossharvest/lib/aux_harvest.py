import json
import codecs
import stringcase
import numbers
import shapely.wkt
import shapely.geometry
import datetime
import re

from dateutil.parser import parse
# from pyproj import Transformer

class AuxHarvester():
    def clean_snakecase(self, og_string):
        og_string = re.sub('[^0-9a-zA-Z]+', '_', og_string).lower()
        sc_string = stringcase.snakecase(og_string).strip("_")
        while "__" in sc_string:
            sc_string = sc_string.replace("__", "_")
        return sc_string

    def check_attributes(self, json_dict, fixed_attributes):
        for fixed_attribute in fixed_attributes:
            attribute_key = fixed_attribute["key"]
            attribute_value = fixed_attribute["value"]
            actual_value = json_dict.get(attribute_key, None)
            if not actual_value or attribute_value != actual_value:
                return None
        return True

    def get_field(self, json_dict, field_path):
        result = []
        if len(field_path) == 0:
            return [json_dict]
        else:
            tag = field_path.pop(0)
            if tag["key"] not in json_dict:
                return None
            elif self.check_attributes(json_dict, tag["fixed_attributes"]):
                new_json_dict = json_dict[tag["key"]]
                if type(new_json_dict) is list:
                    for list_entry in new_json_dict:
                        # Python3
                        #value = self.get_field(list_entry, field_path.copy())
                        # Python2
                        value = self.get_field(list_entry, field_path[:])
                        if value:
                            result.extend(value)
                else:
                    # Python3
                    #value = self.get_field(new_json_dict, field_path.copy())
                    # Python3
                    value = self.get_field(new_json_dict, field_path[:])
                    if value:
                        result.extend(value)
                return result
            else:
                return None

    def _parse_resources(self, content, resource_fields):
        resources = []
        for resource in resource_fields:
            single_resource = {}
            for key, field in resource.items():
                if field["field_type"] == "freeText":
                    single_resource[key] = field["freeText"]
                elif field["field_type"] == "path":
                    field_value = self.get_field(content, field["path"][:])
                    if field_value:
                        single_resource[key] = field_value[0]
                    else:
                        break
            resources.append(single_resource)
        return resources

    def _parse_extras(self, content, extra_fields):
        extras = {}
        for extra in extra_fields:
            field_name = self.clean_snakecase(extra["field_name"])
            field_value = self.get_field(content, extra["path"][:])
            if field_value:
                extras.update({field_name: field_value[0]})
        return extras

    def tag_parsing(self, tag_list):
        ckan_tag_list = []
        for tag in tag_list:
            str_tag = str(tag)
            if len(str_tag)<=3 and len(str_tag)>0:
                ckan_tag_list.append({"name": str(tag)})
        return ckan_tag_list

    def temporal_parsing(self, date, parsing_function, end=0):
        start_default = datetime.datetime(2020,1,1,0,0,0)
        end_default = datetime.datetime(2020,12,31,23,59,59)
        default_date = end_default if end else start_default

        if parsing_function == "SingleDate":
            return parse(date, default=default_date).strftime("%Y-%m-%dT%H:%M:%SZ")
        elif parsing_function == "SlashDate":
            start_date, end_date = date.split("/")
            start_date = parse(start_date, default=default_date).strftime("%Y-%m-%dT%H:%M:%SZ")
            end_date = parse(end_date, default=default_date).strftime("%Y-%m-%dT%H:%M:%SZ")
            return end_date if end else start_date
        else:
            raise ModuleNotFoundError("{} is not a recognized date parsing function".format(parsing_function))


    def spatial_parsing(self, spatial, parsing_function):
        if spatial:
            geojson = None
            if parsing_function == "GeoJSON":
                geojson = spatial[0]

            elif parsing_function == "WKT":
                shapely_wkt = shapely.wkt.loads(spatial[0])
                geojson = shapely.geometry.mapping(shapely_wkt)

            elif parsing_function == "CardinalPoints":
                lon1, lat1, lon2, lat2 = spatial
                geojson = self.bbcoords2geometry(float(lon1),float(lat1),
                                               float(lon2),float(lat2))

            elif parsing_function == "BboxDiffTagSpace":
                lon1, lat1 = spatial[0].split(" ")
                lon2, lat2 = spatial[1].split(" ")
                geojson = self.bbcoords2geometry(float(lon1),float(lat1),
                                               float(lon2),float(lat2))
            else:
                raise ModuleNotFoundError("{} is not a recognized spatial parsing function".format(parsing_function))
            return json.dumps(geojson)

    def bbcoords2geometry(self, min_long, min_lat, max_long, max_lat):
        shapely_polygon = shapely.geometry.Polygon([(min_long, min_lat),
                                                    (min_long, max_lat),
                                                    (max_long, max_lat),
                                                    (max_long, min_lat)])
                                                    
        return json.loads(json.dumps(shapely.geometry.mapping(shapely_polygon)))

"""
    # Current version of pyproj does not have the property Transformer, in order
    # to upgrade the version, it is also required to upgrade the version of 
    # PROJ (OS package), which breaks other python libraries
    # NOTE: This only happens due to the fact that python2 is deprecated
    def project_coords(self, coords, from_proj, to_proj):
        if len(coords) < 1:
            return []

        if isinstance(coords[0], numbers.Number):
            transformer = Transformer.from_crs(from_proj, to_proj)
            from_x, from_y = coords
            # There is a known issue where with the new syntax
            # ("EPSG:4326"), PROJ honors the axis order of the CRS
            # definition (which is lat, lon), while before it assumed
            # an x, y (so lon, lat) order.
            to_x, to_y = transformer.transform(from_x, from_y)
            return [to_y, to_x]

        new_coords = []
        for coord in coords:
            new_coords.append(self.project_coords(coord, from_proj, to_proj))
        return new_coords
    
    def project_geometry(self, geometry, from_proj, to_proj):
        if 'coordinates' not in geometry:
            print('Failed project feature')
            return None

        new_coordinates = self.project_coords(geometry['coordinates'], from_proj, to_proj)
        geometry['coordinates'] = new_coordinates
        return geometry
"""