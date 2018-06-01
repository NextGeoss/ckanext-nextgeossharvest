"""Tests for nextgeoss_base.py."""
from ..harvesters.probav import PROBAVHarvester, Units, ProductType, L2AProbaVCollection, SProbaVCollection, Resolution, Session
from unittest import TestCase
from os import path
from bs4 import BeautifulSoup
import json
import re
import mock
from ckanext.harvest.model import HarvestObject
from datetime import date

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
        metalink_entries = self.harvester._get_metalink_file_elements(metalinks)
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

def read_entries(filename):
    filepath = path.join(path.dirname(__file__), filename)
    with open(filepath, 'r') as open_search_file:
        open_search_resp = BeautifulSoup(open_search_file.read(), 'lxml-xml')
        return open_search_resp


class TestProbavHarvester(TestCase):

    def test_generate_harvest_url(self):
        url = self.harvester._generate_harvest_url('PROBAV_L2A_333M_V001', date(2018, 1, 1), date(2018, 1, 2) )
        self.assertEqual(url, 'http://www.vito-eodata.be/openSearch/findProducts.atom?collection=urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001&platform=PV01&start=2018-01-01&end=2018-01-02&count=500')

    def test_get_entries_from_results(self):
        entries_from_file = read_entries('l2a_500_entries.xml')
        self.harvester._init()
        entry_iter = self.harvester._parse_open_search_entries(entries_from_file)
        self.maxDiff = None
        self.assertEqual(entry_iter[0].identifier.string, 'urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101')
        self.assertEqual(len(entry_iter), 158)


    def setUp(self):
        self.entry = read_first_entry('l2a_entry.xml')
        self.harvester = PROBAVHarvester()
    
    def test_gather_L2A(self):
        harvest_objects_iterator = self.harvester._gather_L2A('http://www.vito-eodata.be/openSearch/findProducts.atom?collection=urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001&platform=PV01&start=2018-01-01&end=2018-01-02&count=500')
        harvest_objects = list(harvest_objects_iterator)
        firts_havest_object = harvest_objects[0]
        self.assertEqual(firts_havest_object['guid'], 'urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101')
        self.assertEqual(len(harvest_objects), 158)

    def test_gather_L3(self):
        harvest_objects_iterator = self.harvester._gather_L3('http://www.vito-eodata.be/openSearch/findProducts.atom?collection=urn:ogc:def:EOP:VITO:PROBAV_S1-TOA_333M_V001&platform=PV01&start=2018-01-01&end=2018-01-02&count=500')
        harvest_objects = list(harvest_objects_iterator)
        first_havest_object = harvest_objects[0]
        self.assertEqual(first_havest_object['guid'], 'urn:ogc:def:EOP:VITO:PROBAV_S1-TOA_333M_V001:PROBAV_S1-TOA_20180101_333M:V101:PROBAV_S1_TOA_X00Y00_20180101_333M_V101.HDF5')
        self.assertEqual(len(harvest_objects), 331 + 331)

    def test_create_harvest_object(self):
        harvest_object = self.harvester._create_harvest_object('GUID:1', 'restart_date', 'content', extras={'k1': 1, 'k2': 2})
        content = json.loads(harvest_object['content'])
        del harvest_object['content']
        self.assertDictEqual(harvest_object, {
            'identifier': 'guid_1',
            'guid': 'GUID:1',
            'restart_date': 'restart_date'
        })
        self.assertDictEqual(content, {
            'content': 'content',
            'extras': {'k1': 1, 'k2': 2}
        })

    def test_create_harvest_object_and_gather_entry_integration(self):
        from sqlalchemy.orm.query import Query
        # from ckanext.harvest.model import HarvestObject
    
        with mock.patch.object(Query, 'first', return_value=None):
            with mock.patch.object(HarvestObject, 'save'):
                self.harvester.job = None
                harvest_object = self.harvester._create_harvest_object('GUID:1', 'restart_date', 'content', extras={'k1': 1, 'k2': 2})
                ids = self.harvester._gather_entry(harvest_object)
                self.assertListEqual(ids, ['GUID:1'])
                


    def test_parse_items_per_page(self):
        items_per_page = self.harvester._parse_items_per_page(read_entries('l2a_500_entries.xml'))
        self.assertEqual(items_per_page, 158)

    def test_open_search_pages_from(self):
        open_search_pages = self.harvester._open_search_pages_from('http://www.vito-eodata.be/openSearch/findProducts.atom?collection=urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001&platform=PV01&start=2018-01-01&end=2018-01-02&count=500')
        open_search_first_page = next(open_search_pages)
        self.assertIsInstance(open_search_first_page, BeautifulSoup)
        self.assertEqual(open_search_first_page.find('totalResults').string, '158')

    def test_get_xml_url(self):
        xml = self.harvester._get_xml_from_url('https://www.vito-eodata.be/PDF/dataaccess?service=DSEO&request=GetProduct&version=1.0.0&collectionID=1000112&productID=267469925&ProductURI=urn:ogc:def:EOP:VITO:PROBAV_S1-TOA_333M_V001:PROBAV_S1-TOA_20180101_333M:V101&', auth=('nextgeoss', 'nextgeoss'))
        self.assertIsInstance(xml, BeautifulSoup)
        self.assertIsNotNone(xml.files.file)

    def test_generate_L3_guid(self):
        guid = self.harvester._generate_L3_guid('urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101', 'PROBAV_S1_TOA_20180101_333M_V101')
        self.assertEqual(guid, 'urn:ogc:def:EOP:VITO:PROBAV_L2A_333M_V001:PROBAV_CENTER_L2A_20180101_005544_333M:V101:PROBAV_S1_TOA_20180101_333M_V101')

    def test_parse_restart_date(self):
        restart_date = self.harvester._parse_restart_date(self.entry)
        self.assertEqual(restart_date, "2018-01-01T04:55:43Z")

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

 
