"""Tests for plugin.py."""

import json
import os

from bs4 import BeautifulSoup as Soup

from ckanext.nextgeossharvest.lib.esa_base import SentinelHarvester


class TestConvertToGeoJSON(object):
    """Tests for the _convert_to_geojson() method."""

    def test_detect_polygon(self):
        coords_1 = 'POLYGON ((-12.947799 27.343195,-15.513319 27.760723,-15.196670 29.384781,-12.589683 28.970003,-12.947799 27.343195))'  # noqa: E501
        coords_2 = 'POINT(-12.947799 27.343195)'
        coords_3 = 'POLYGON ((10 10, 10))'

        assert SentinelHarvester()._convert_to_geojson(coords_1) is not None
        assert SentinelHarvester()._convert_to_geojson(coords_2) is None
        assert SentinelHarvester()._convert_to_geojson(coords_3) is None

    def test_is_geojson(self):
        coords = 'POLYGON ((-12.947799 27.343195,-15.513319 27.760723,-15.196670 29.384781,-12.589683 28.970003,-12.947799 27.343195))'  # noqa: E501
        geojson = SentinelHarvester()._convert_to_geojson(coords)

        assert json.loads(geojson)
        geo_dict = json.loads(geojson)
        assert geo_dict['type'] == 'Polygon'
        assert geo_dict['coordinates'][0] == [[-12.947799, 27.343195], [-15.513319, 27.760723], [-15.19667, 29.384781], [-12.589683, 28.970003], [-12.947799, 27.343195]]  # noqa: E501


class TestNormalizeNames(object):
    """Tests for the _normalize_names() method."""

    def test_normalize_sentinel_1(self):
        directory = os.path.dirname(os.path.abspath(__file__))
        file = os.path.join(directory, 'feeds/sentinel-1-results-feed.xml')
        with open(file, 'r') as f:
            soup = Soup(f, 'lxml')

        test_entry = soup.find_all('entry')[0]
        test_item = SentinelHarvester()._normalize_names(test_entry)

        expected_item = {
            'StartPosition': '2018-01-31T10:47:13.362Z',
            'StopPosition': '2018-01-31T10:48:13.359Z',
            'Filename': 'S1B_EW_GRDH_1SDH_20180131T104713_20180131T104813_009414_010EA4_BD6D.SAFE',  # noqa: E501
            'spatial': 'POLYGON ((-53.462952 71.570305,-65.216614 72.590927,-62.853455 76.137955,-48.577496 74.941811,-53.462952 71.570305))',  # noqa: E501
            'FamilyName': 'Sentinel-1',
            'InstrumentFamilyName': 'SAR-C SAR',
            'InstrumentName': 'Synthetic Aperture Radar (C-band)',
            'TransmitterReceiverPolarisation': 'HH HV',
            'InstrumentMode': 'EW',
            'ProductClass': 'S',
            'ProductType': 'GRD',
            'AcquisitionType': 'NOMINAL',
            'OrbitDirection': 'ASCENDING',
            'Swath': 'EW',
            'uuid': '02f44244-1c35-481b-9357-764277d949ef',
            'Identifier': 'S1B_EW_GRDH_1SDH_20180131T104713_20180131T104813_009414_010EA4_BD6D',  # noqa: E501
        }

        assert len(test_item) == len(expected_item) == 16
        assert test_item['StartPosition'] == expected_item['StartPosition']
        assert test_item['StopPosition'] == expected_item['StopPosition']
        assert test_item['Filename'] == expected_item['Filename']
        assert test_item['spatial'] == expected_item['spatial']
        assert test_item['FamilyName'] == expected_item['FamilyName']
        assert test_item['InstrumentFamilyName'] == expected_item['InstrumentFamilyName']  # noqa: E501
        assert test_item['InstrumentName'] == expected_item['InstrumentName']
        assert test_item['TransmitterReceiverPolarisation'] == expected_item['TransmitterReceiverPolarisation']  # noqa: E501
        assert test_item['InstrumentMode'] == expected_item['InstrumentMode']
        assert test_item['ProductClass'] == expected_item['ProductClass']
        assert test_item['ProductType'] == expected_item['ProductType']
        assert test_item['AcquisitionType'] == expected_item['AcquisitionType']
        assert test_item['OrbitDirection'] == expected_item['OrbitDirection']
        assert test_item['Swath'] == expected_item['Swath']
        assert test_item['uuid'] == expected_item['uuid']
        assert test_item['Identifier'] == expected_item['Identifier']
        assert test_item == expected_item
