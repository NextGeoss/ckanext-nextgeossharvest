"""Tests for nextgeoss_base.py."""
from ..harvesters.probav import PROBAVHarvester, Units, ProductType, L2AProbaVCollection, SProbaVCollection, Resolution
from unittest import TestCase
from os import path
from bs4 import BeautifulSoup
import json
import re

class TestProvaVCollection(TestCase):

    def test_l2a_collection_str(self):
        collection = L2AProbaVCollection(ProductType.L2A, Resolution(333, Units.METERS))
        self.assertEqual(str(collection), 'PROBAV_L2A_333M_V001') 
    
    def test_get_name(self):
        collection = L2AProbaVCollection(ProductType.L2A, Resolution(333, Units.METERS))
        self.assertEqual(collection.get_name(), 'Proba-V Level-2A (333M)')
    
    def test_get_description(self):
        collection = L2AProbaVCollection(ProductType.L2A, Resolution(333, Units.METERS))
        self.assertEqual(collection.get_description(), 'PROBA-V Level2A - 333M segments contain the Level 1C (P product) data projected on a uniform 333m grid.') 
    
    def test_get_tags(self):
        collection = L2AProbaVCollection(ProductType.L2A, Resolution(333, Units.METERS))
        self.assertListEqual(collection.get_tags(), ['Proba-V', 'L2A', '333M'])

class TestSProvaVCollection(TestCase):

    def test_l2a_collection_str(self):
        collection = SProbaVCollection(1, ProductType.TOA, Resolution(333, Units.METERS), False)
        self.assertEqual(str(collection), 'PROBAV_S1-TOA_333M_V001') 
    
    def test_get_name(self):
        collection = SProbaVCollection(1, ProductType.TOA, Resolution(333, Units.METERS), False)
        self.assertEqual(collection.get_name(), 'Proba-V S1-TOA (333M)')
    
    def test_get_description(self):
        collection = SProbaVCollection(1, ProductType.TOA, Resolution(333, Units.METERS), False)
        self.assertEqual(collection.get_description(), 'Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame of 1 day for 333m of spatial resolution.') 
    
    def test_get_tags(self):
        collection = SProbaVCollection(1, ProductType.TOA, Resolution(333, Units.METERS), False)
        self.assertListEqual(collection.get_tags(), ['Proba-V', 'S1-TOA', '333M'])

METALINK_URL = "https://www.vito-eodata.be/PDF/dataaccess?service=DSEO&request=GetProduct&version=1.0.0&collectionID=1000125&productID=267473044&ProductURI=urn:ogc:def:EOP:VITO:PROBAV_S1-TOA_100M_V001:PROBAV_S1-TOA_20180101_100M:V101&fileIndex=1"

class TestSProbavHarvester(TestCase):

    def setUp(self):
        self.entry = read_first_entry('s1_100m.xml')
        self.harvester = PROBAVHarvester()
        self.file = read_first_file('metalink.xml')

    def test_parse_metalink_url(self):
        url = self.harvester._parse_metalink_url(self.entry)
        self.assertEqual(url, "https://www.vito-eodata.be/PDF/dataaccess?service=DSEO&request=GetProduct&version=1.0.0&collectionID=1000125&productID=267473044&ProductURI=urn:ogc:def:EOP:VITO:PROBAV_S1-TOA_100M_V001:PROBAV_S1-TOA_20180101_100M:V101&")

    def test_parse_file_name(self):
        filename = self.harvester._parse_file_name(self.file)
        self.assertEqual(filename, "PROBAV_S1_TOA_X00Y00_20180101_100M_V101.HDF5")

    def test_parse_S_identifier(self):
        file_name = self.harvester._parse_S_identifier("PROBAV_S1_TOA_X00Y00_20180101_100M_V101.HDF5")
        self.assertEqual(file_name, "PROBAV_S1_TOA_X00Y00_20180101_100M_V101")

    def test_parse_S_name(self):
        file_name = self.harvester._parse_S_name("PROBAV_S1_TOA_X00Y00_20180101_100M_V101.HDF5")
        self.assertEqual(file_name, "probav_s1_toa_x00y00_20180101_100m_v101")

    def test_parse_file_url(self):
        url = self.harvester._parse_file_url(self.file)
        self.assertEqual(url, METALINK_URL)

    def test_parse_coordinates(self):
        coordinates = self.harvester._parse_coordinates("PROBAV_S1_TOA_X02Y09_20180101_100M_V101.HDF5")
        self.assertEqual(coordinates, (2, 9))

    def test_generate_bbox(self):
        x, y = (2, 1)
        bbox = self.harvester._generate_bbox((x, y))
        lng_min = -160
        lng_max = -150
        lat_max = 65
        lat_min = 55
        self.assertEqual(bbox, [lat_min, lng_min, lat_max, lng_max])
    
    def test_generate_tile_thumbnail(self):
        thumbnail_url = "https://www.vito-eodata.be/cgi-bin/probav?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&SRS=EPSG:4326&FORMAT=image/png&LAYERS=PROBAV_L3_S1_TOA_100M_Red band&TIME=2018-01-01T00:00:00Z&BBOX=-54.999,-180.0,75.0,179.998992&HEIGHT=200&WIDTH=554"
        tile_url = self.harvester._generate_tile_thumbnail_url(thumbnail_url, [65, -160, 75, -150])
        self.assertEqual(tile_url, 'https://www.vito-eodata.be/cgi-bin/probav?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&SRS=EPSG:4326&FORMAT=image/png&LAYERS=PROBAV_L3_S1_TOA_100M_Red+band&TIME=2018-01-01T00:00:00Z&BBOX=65,-160,75,-150&HEIGHT=10&WIDTH=10')

    def test_parse_S_content(self):
        content = {
            'opensearch_entry': str(self.entry),
            'file_entry': str(self.file)
        }
        parsed_content = self.harvester._parse_content(json.dumps(content))
        self.assertIsNotNone(parsed_content.get('uuid'))
        del parsed_content['uuid']
        expected_parsed_content = {
            'title': 'Proba-V S1-TOA (100M)',
            'description': 'Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame of 1 day for 100m of spatial resolution.',
            'tags': [
                {'name': 'Proba-V'},
                {'name': 'S1-TOA'},
                {'name': '100M'}
            ],
            'identifier': 'PROBAV_S1_TOA_X00Y00_20180101_100M_V101',
            'StartTime': '2018-01-01T00:00:00Z',
            'StopTime': '2018-01-01T23:59:59Z',
            'Collection': 'PROBAV_S1-TOA_100M_V001',
            'name': 'probav_s1_toa_x00y00_20180101_100m_v101',
            'filename': 'PROBAV_S1_TOA_X00Y00_20180101_100M_V101.HDF5',
            'notes': 'Synthesis products with Top of Atmosphere (TOA) reflectances composited over defined time frame of 1 day for 100m of spatial resolution.',
            'metadata_download': "https://www.vito-eodata.be/PDF/dataaccessMdXML?mdmode=hma&collectionID=1000125&productID=267473044&fileName=PV_S1_TOA-20180101_100M_V101.xml",
            'product_download': "https://www.vito-eodata.be/PDF/dataaccess?service=DSEO&request=GetProduct&version=1.0.0&collectionID=1000125&productID=267473044&ProductURI=urn:ogc:def:EOP:VITO:PROBAV_S1-TOA_100M_V001:PROBAV_S1-TOA_20180101_100M:V101&fileIndex=1",
            'thumbnail_download': "https://www.vito-eodata.be/cgi-bin/probav?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&SRS=EPSG:4326&FORMAT=image/png&LAYERS=PROBAV_L3_S1_TOA_100M_Red+band&TIME=2018-01-01T00:00:00Z&BBOX=65,-180,75,-170&HEIGHT=10&WIDTH=10"
        }
        lng_min = -180
        lat_min = 65
        lng_max = -170
        lat_max = 75
        bbox = [[lng_min,lat_max],
                [lng_max,lat_max],
                [lng_max,lat_min],
                [lng_min, lat_min],
                [lng_min,lat_max]]
        expected_spatial = {"type":"Polygon","crs":{"type":"EPSG","properties":{"code":4326,"coordinate_order":"Long,Lat"}},"coordinates":[bbox]}
        spatial = json.loads(parsed_content['spatial'])
        del parsed_content['spatial']
        self.maxDiff = None
        self.assertDictEqual(parsed_content, expected_parsed_content)
        self.assertDictEqual(spatial, expected_spatial)
    
    def test_get_metalink_file_entries(self):
        metalinks = read_metalink_file('metalink.xml')
        metalink_entries = self.harvester._get_metalink_file_entries(metalinks)
        self.assertEqual(len(list(metalink_entries)), 196)
        self.assertEqual(metalink_entries[0]['name'], "PROBAV_S1_TOA_X00Y00_20180101_100M_V101.HDF5")
        self.assertTrue(all(file_entry['name'].endswith('.HDF5') for file_entry in metalink_entries))

HDF5_FILENAME_REGEX = re.compile('.*\.HDF5$')


def read_metalink_file(filename):
    filepath = path.join(path.dirname(__file__), filename)
    with open(filepath, 'r') as open_search_file:
        return BeautifulSoup(open_search_file.read(), 'lxml-xml')


def read_first_file(filename):
    filepath = path.join(path.dirname(__file__), filename)
    with open(filepath, 'r') as open_search_file:
        open_search_resp = BeautifulSoup(open_search_file.read(), 'lxml-xml')
        elem = open_search_resp.files.find(name='file', attrs={'name': HDF5_FILENAME_REGEX})
        print(str(elem))
        return elem


def read_first_entry(filename):
    filepath = path.join(path.dirname(__file__), filename)
    with open(filepath, 'r') as open_search_file:
        open_search_resp = BeautifulSoup(open_search_file.read(), 'lxml-xml')
        return open_search_resp.entry


class TestProbavHarvester(TestCase):

    def setUp(self):
        self.entry = read_first_entry('l2a_entry.xml')
        self.harvester = PROBAVHarvester()

    def test_create_contents_json(self):
        json_string = self.harvester._create_contents_json('os entry', 'metalink entry')
        self.assertDictEqual(json.loads(json_string), {'opensearch_entry': 'os entry', 'file_entry': 'metalink entry'})

    def test_parse_identifier_element(self):
        identifier = self.harvester._parse_identifier_element(self.entry)
        self.assertEqual(identifier, 'urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101')

    def test_parse_interval(self):
        start, end = self.harvester._parse_interval(self.entry)
        self.assertEqual(start, "2018-01-01T00:55:44Z")
        self.assertEqual(end, "2018-01-01T01:02:21Z")

    def test_parse_identifier(self):
        identifier = 'urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101'
        identifier = self.harvester._parse_identifier(identifier)
        self.assertEqual(identifier, 'PROBAV_CENTER_L2A_20180101_005544_333M_V101')

    def test_parse_name(self):
        identifier = 'urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101'
        name = self.harvester._parse_name(identifier)
        self.assertEqual(name, 'probav_center_l2a_20180101_005544_333m_v101')

    def test_parse_filename(self):
        identifier = 'urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101'
        filename = self.harvester._parse_filename(identifier)
        self.assertEqual(filename, 'PROBAV_CENTER_L2A_20180101_005544_333M_V101.HDF5')

    def test_prase_bounding_box(self):
        bbox = self.harvester._parse_bbox(self.entry)
        self.assertEqual(bbox, [40.341, 145.476, 65.071, 165.962992])

    def test_prase_bounding_box_to_geojson(self):
        bbox = [40.341, 145.476, 65.071, 165.962992]
        geojson = self.harvester._bbox_to_geojson(bbox)
        self.assertDictEqual(geojson, {
            'type': 'Polygon',
            'crs': {
                'type': 'EPSG',
                'properties': {
                    'coordinate_order': 'Long,Lat',
                    'code': 4326
                },
            },
            'coordinates': [[[145.476, 65.071],
                             [165.962992, 65.071],
                             [165.962992, 40.341],
                             [145.476, 40.341],
                             [145.476, 65.071]]]
            })

    def test_bbox_to_polygon(self):
        bbox = [40.341, 145.476, 65.071, 165.962992]
        polygon = self.harvester._bbox_to_polygon(bbox)
        self.assertEqual(polygon,
                         [[145.476, 65.071],
                          [165.962992, 65.071],
                          [165.962992, 40.341],
                          [145.476, 40.341],
                          [145.476, 65.071]])

    def test_parse_L2A_collection_from_identifier(self):
        identifier = 'urn:ogc:def:EOP:VITO:PROBAV_L2A_100M_V001:PROBAV_CENTER_L2A_20180101_005544_100M:V101'
        collection  = self.harvester._parse_collection_from_identifier(identifier)
        self.assertIsInstance(collection, L2AProbaVCollection)
        self.assertEqual(collection.product_type, ProductType.L2A)
        self.assertEqual(collection.resolution.units, Units.METERS)
        self.assertEqual(collection.resolution.value, 100)

    def test_parse_S1_TOC_NDVI_collection_from_identifier(self):
        identifier = 'urn:ogc:def:EOP:VITO:PROBAV_S1-TOC-NDVI_100M_V001:PROBAV_S1-TOC-NDVI_20180101_100M:V101'
        collection  = self.harvester._parse_collection_from_identifier(identifier)
        self.assertIsInstance(collection, SProbaVCollection)
        self.assertEqual(collection.frequency, 1)
        self.assertEqual(collection.product_type, ProductType.TOC)
        self.assertEqual(collection.resolution.units, Units.METERS)
        self.assertEqual(collection.resolution.value, 100)

    def test_parse_L2A_content(self):
        content = {'opensearch_entry': str(self.entry)}
        parsed_content = self.harvester._parse_content(json.dumps(content))
        self.assertIsNotNone(parsed_content.get('uuid'))
        del parsed_content['uuid']
        expected_parsed_content = {
            'title': 'Proba-V Level-2A (333M)',
            'description': 'PROBA-V Level2A - 333M segments contain the Level 1C (P product) data projected on a uniform 333m grid.',
            'tags': [
                {'name': 'Proba-V'},
                {'name': 'L2A'},
                {'name': '333M'}
            ],
            'identifier': 'PROBAV_CENTER_L2A_20180101_005544_333M_V101',
            'StartTime': '2018-01-01T00:55:44Z',
            'StopTime': '2018-01-01T01:02:21Z',
            'Collection': 'PROBAV_L2A_333M_V001',
            'name': 'probav_center_l2a_20180101_005544_333m_v101',
            'filename': 'PROBAV_CENTER_L2A_20180101_005544_333M_V101.HDF5',
            'notes': 'PROBA-V Level2A - 333M segments contain the Level 1C (P product) data projected on a uniform 333m grid.',
            'metadata_download': "https://www.vito-eodata.be/PDF/dataaccessMdXML?mdmode=hma&collectionID=1000126&productID=267446487&fileName=PV_CENTER_L2A-20180101005544_333M_V101.xml",
            'product_download': "https://www.vito-eodata.be/PDF/dataaccess?service=DSEO&request=GetProduct&version=1.0.0&collectionID=1000126&productID=267446487&ProductURI=urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101&",
            'thumbnail_download': "https://www.vito-eodata.be/cgi-bin/probav?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&SRS=EPSG:4326&FORMAT=image/png&LAYERS=PROBAV_L2A_333M_Red band&TIME=2018-01-01T00:55:44Z&BBOX=40.341,145.476,65.071,165.962992&HEIGHT=200&WIDTH=166"
        }
        expected_spatial = {"type":"Polygon","crs":{"type":"EPSG","properties":{"code":4326,"coordinate_order":"Long,Lat"}},"coordinates":[[[145.476,65.071], [165.962992,65.071], [165.962992,40.341], [145.476, 40.341], [145.476,65.071]]]}
        spatial = json.loads(parsed_content['spatial'])
        del parsed_content['spatial']
        self.maxDiff = None
        self.assertDictEqual(parsed_content, expected_parsed_content)
        self.assertDictEqual(spatial, expected_spatial)

    def test_get_metadata_url(self):
        metadata_url = self.harvester._get_metadata_url(self.entry)
        self.assertEqual(metadata_url, "https://www.vito-eodata.be/PDF/dataaccessMdXML?mdmode=hma&collectionID=1000126&productID=267446487&fileName=PV_CENTER_L2A-20180101005544_333M_V101.xml")

    def test_get_product_url(self):
        product_url = self.harvester._get_product_url(self.entry)
        self.assertEqual(product_url, "https://www.vito-eodata.be/PDF/dataaccess?service=DSEO&request=GetProduct&version=1.0.0&collectionID=1000126&productID=267446487&ProductURI=urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101&")

    def test_get_thumbnail_url(self):
        thumbnail_url = self.harvester._get_thumbnail_url(self.entry)
        self.assertEqual(thumbnail_url, "https://www.vito-eodata.be/cgi-bin/probav?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&SRS=EPSG:4326&FORMAT=image/png&LAYERS=PROBAV_L2A_333M_Red band&TIME=2018-01-01T00:55:44Z&BBOX=40.341,145.476,65.071,165.962992&HEIGHT=200&WIDTH=166")

    def test_get_resources(self):
        content = {'opensearch_entry': str(self.entry)}
        parsed_content = self.harvester._parse_content(json.dumps(content))
        resources = self.harvester._get_resources(parsed_content)
        self.assertListEqual(resources, [
            {
                'name': 'Metadata Download',
                'url': "https://www.vito-eodata.be/PDF/dataaccessMdXML?mdmode=hma&collectionID=1000126&productID=267446487&fileName=PV_CENTER_L2A-20180101005544_333M_V101.xml",
                'format': 'xml',
                'mimetype': 'application/xml'
            },
            {
                'name': 'Product Download',
                'url': "https://www.vito-eodata.be/PDF/dataaccess?service=DSEO&request=GetProduct&version=1.0.0&collectionID=1000126&productID=267446487&ProductURI=urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101&",
                'format': 'hdf5',
                'mimetype': 'application/x-hdf5'
            },
            {
                'name': 'Thumbnail Download',
                'url': "https://www.vito-eodata.be/cgi-bin/probav?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&SRS=EPSG:4326&FORMAT=image/png&LAYERS=PROBAV_L2A_333M_Red band&TIME=2018-01-01T00:55:44Z&BBOX=40.341,145.476,65.071,165.962992&HEIGHT=200&WIDTH=166",
                'format': 'png',
                'mimetype': 'image/png'
            }
        ])

 
