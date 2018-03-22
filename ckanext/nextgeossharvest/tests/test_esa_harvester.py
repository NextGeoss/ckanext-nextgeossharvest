"""Tests for nextgeoss_base.py."""

import json
import os

from bs4 import BeautifulSoup as Soup

from ckanext.nextgeossharvest.harvesters.esa import ESAHarvester


class TestESAHarvester(object):
    """Tests for the ESAHarvester class."""

    def __init__(self):
        directory = os.path.dirname(os.path.abspath(__file__))
        file = os.path.join(directory, 'feeds/sentinel-1-results-feed.xml')
        with open(file, 'r') as f:
            self.one_page_of_results = Soup(f, 'lxml')
        self.harvester = ESAHarvester()
        self.harvester.os_id_name = 'str',
        self.harvester.os_id_attr = {'name': 'identifier'}
        self.harvester.os_guid_name = 'str'
        self.harvester.os_guid_attr = {'name': 'uuid'}
        self.harvester.os_restart_date_name = 'date'
        self.harvester.os_restart_date_attr = {'name': 'ingestiondate'}
        self.harvester.os_restart_filter = None
        self.harvester.flagged_extra = 'scihub_download_url'

    def test_get_entries_from_results(self):
        entries = self.harvester._get_entries_from_results(self.one_page_of_results)
        assert len(entries) == 10
        assert entries[0]['content'].startswith('<entry>\n<title>S1B_EW_GRDH_1SDH_20180131T104713_20180131T104813_009414_010EA4_BD6D</title>')

    def test_parse_content(self):
        entries = self.harvester._get_entries_from_results(self.one_page_of_results)
        parsed_content = self.harvester._parse_content(entries[0]['content'])
        # Replace this with a complete dictionary and assert ==.
        assert parsed_content['name'] == 'S1B_EW_GRDH_1SDH_20180131T104713_20180131T104813_009414_010EA4_BD6D'.lower()
