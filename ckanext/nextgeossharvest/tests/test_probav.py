"""Tests for nextgeoss_base.py."""
from ..harvesters.probav import PROBAVHarvester, Units, ProductType, L2AProbaVCollection, SProbaVCollection, Resolution
from unittest import TestCase
from os import path
from bs4 import BeautifulSoup

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
    
class TestProbavHarvester(TestCase):

    def test_parse_identifier_element(self):
        entry = self.read_first_entry('l2a_entry.xml')
        harvester = PROBAVHarvester()
        identifier = harvester._parse_identifier_element(entry)
        self.assertEqual(identifier, 'urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101')

    def test_parse_interval(self):
        entry = self.read_first_entry('l2a_entry.xml')
        harvester = PROBAVHarvester()
        start, end = harvester._parse_interval(entry)
        self.assertEqual(start, "2018-01-01T00:55:44Z")
        self.assertEqual(end, "2018-01-01T01:02:21Z")
    
    def test_parse_identifier(self):
        identifier = 'urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101'
        harvester = PROBAVHarvester()
        identifier = harvester._parse_identifier(identifier)
        self.assertEqual(identifier, 'PROBAV_CENTER_L2A_20180101_005544_333M_V101')

    def test_parse_name(self):
        identifier = 'urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101'
        harvester = PROBAVHarvester()
        name = harvester._parse_name(identifier)
        self.assertEqual(name, 'PROBAV_CENTER_L2A_20180101_005544_333M_V101.HDF5')
    
    def test_prase_bounding_box(self):
        entry = self.read_first_entry('l2a_entry.xml')
        harvester = PROBAVHarvester()
        bbox = harvester._parse_bbox(entry)
        self.assertEqual(bbox, [40.341, 145.476, 65.071, 165.962992])

    def test_prase_bounding_box_to_geojson(self):
        bbox = [40.341, 145.476, 65.071, 165.962992]
        harvester = PROBAVHarvester()
        geojson = harvester._bbox_to_geojson(bbox)
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
        harvester = PROBAVHarvester()
        polygon = harvester._bbox_to_polygon(bbox)
        self.assertEqual(polygon, 
                         [[145.476, 65.071],
                          [165.962992, 65.071],
                          [165.962992, 40.341],
                          [145.476, 40.341],
                          [145.476, 65.071]])

    def test_parse_L2A_collection_from_identifier(self):
        identifier = 'urn:ogc:def:EOP:VITO:PROBAV_L2A_100M_V001:PROBAV_CENTER_L2A_20180101_005544_100M:V101'
        harvester = PROBAVHarvester()
        collection  = harvester._parse_collection_from_identifier(identifier)
        self.assertIsInstance(collection, L2AProbaVCollection)
        self.assertEqual(collection.product_type, ProductType.L2A)
        self.assertEqual(collection.resolution.units, Units.METERS)
        self.assertEqual(collection.resolution.value, 100)
 
    def test_parse_S1_TOC_NDVI_collection_from_identifier(self):
        identifier = 'urn:ogc:def:EOP:VITO:PROBAV_S1-TOC-NDVI_100M_V001:PROBAV_S1-TOC-NDVI_20180101_100M:V101'
        harvester = PROBAVHarvester()
        collection  = harvester._parse_collection_from_identifier(identifier)
        self.assertIsInstance(collection, SProbaVCollection)
        self.assertEqual(collection.frequency, 1)
        self.assertEqual(collection.product_type, ProductType.TOC)
        self.assertEqual(collection.resolution.units, Units.METERS)
        self.assertEqual(collection.resolution.value, 100)

    def test_parse_L2A_content(self):
        entry = str(self.read_first_entry('l2a_entry.xml'))
        harvester = PROBAVHarvester()
        parsed_content = harvester._parse_content(entry)
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
            'name': 'PROBAV_CENTER_L2A_20180101_005544_333M_V101.HDF5',
            'spatial': {"type":"Polygon","crs":{"type":"EPSG","properties":{"code":4326,"coordinate_order":"Long,Lat"}},"coordinates":[[[145.476,65.071], [165.962992,65.071], [165.962992,40.341], [145.476, 40.341], [145.476,65.071]]]},
        }
        print(parsed_content)
        self.maxDiff = None 
        self.assertDictEqual(parsed_content, expected_parsed_content)

    def read_first_entry(self, filename):
        filepath = path.join(path.dirname(__file__), filename)
        with open(filepath, 'r') as open_search_file:
            open_search_resp = BeautifulSoup(open_search_file.read(), 'lxml-xml')
            return open_search_resp.entry
