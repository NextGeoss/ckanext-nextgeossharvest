"""Tests for esa_base.py."""

import os

from bs4 import BeautifulSoup as Soup

from ckanext.nextgeossharvest.lib.esa_base import SentinelHarvester


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
            'StartTime': '2018-01-31T10:47:13.362Z',
            'StopTime': '2018-01-31T10:48:13.359Z',
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
            'identifier': 'S1B_EW_GRDH_1SDH_20180131T104713_20180131T104813_009414_010EA4_BD6D',  # noqa: E501
            'size': '1.0 GB',
            'PlatformIdentifier': '2016-025A',
            'RelativeOrbitNumber': '113'
        }

        assert test_item == expected_item
