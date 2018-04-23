"""Tests for nextgeoss_base.py."""
import json
import os

import requests_mock
from bs4 import BeautifulSoup as Soup

from ckan import model
import ckan.tests.helpers as helpers
from ckan.logic import get_action

from ckanext.harvest.logic.action.create import harvest_source_create
from ckanext.harvest.logic.action.update import harvest_source_update
from ckanext.harvest.logic.action.get import harvest_source_show
from ckanext.harvest import queue
from ckanext.harvest.tests import lib
from ckanext.harvest.model import HarvestJob

from ckanext.nextgeossharvest.harvesters.esa import ESAHarvester


class TestESAHarvester(object):
    """Tests for the ESAHarvester class."""

    def __init__(self):
        directory = os.path.dirname(os.path.abspath(__file__))
        file = os.path.join(directory, 'feeds/sentinel-1-results-feed.xml')
        with open(file, 'r') as f:
            self.raw_results = f.read()
        self.one_page_of_results = Soup(self.raw_results, 'lxml')
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
        entries = self.harvester._get_entries_from_results(self.one_page_of_results)  # noqa: E501
        assert len(entries) == 10
        assert entries[0]['content'].startswith('<entry>\n<title>S1B_EW_GRDH_1SDH_20180131T104713_20180131T104813_009414_010EA4_BD6D</title>')  # noqa: E501

    def test_parse_content(self):
        entries = self.harvester._get_entries_from_results(self.one_page_of_results)  # noqa: E501
        parsed_content = self.harvester._parse_content(entries[0]['content'])
        # Replace this with a complete dictionary and assert ==.
        assert parsed_content['name'] == 'S1B_EW_GRDH_1SDH_20180131T104713_20180131T104813_009414_010EA4_BD6D'.lower()  # noqa: E501

    def test_harvester(self):
        """
        Test the harvester by running it for real with mocked requests.

        We need to convert some blocks to helper functions or fixtures,
        but this is an easy way to verify that a harvester does what it's
        supposed to over the course of one or more runs, and we should
        build on it for future tests.
        """
        helpers.reset_db()
        context = {}
        context.setdefault('user', 'test_user')
        context.setdefault('ignore_auth', True)
        context['model'] = model
        context['session'] = model.Session
        user = {}
        user['name'] = 'test_user'
        user['email'] = 'test@example.com'
        user['password'] = 'testpassword'
        helpers.call_action('user_create', context, **user)
        org = {
            'name': 'test_org',
            'url': 'https://www.example.com'
        }

        owner_org = helpers.call_action('organization_create', context, **org)

        config_dict = {'source': 'esa_scihub',
                       'update_all': False,
                       'datasets_per_job': 10,
                       'timeout': 10,
                       'skip_raw': False}
        config = json.dumps(config_dict)
        source = {
            'url': 'http://www.scihub.org',
            'name': 'scihub_test_harvester',
            'owner_org': owner_org['id'],
            'source_type': 'esasentinel',
            'config': config
        }
        harvest_source_create(context, source)
        source = harvest_source_show(context, {'id': 'scihub_test_harvester'})
        job_dict = get_action('harvest_job_create')(
            context, {'source_id': source['id']})
        job_obj = HarvestJob.get(job_dict['id'])
        harvester = queue.get_harvester(source['source_type'])
        with requests_mock.Mocker(real_http=True) as m:
            m.register_uri('GET', '/dhus/search?q', text=self.raw_results)
            lib.run_harvest_job(job_obj, harvester)
        source = harvest_source_show(context, {'id': 'scihub_test_harvester'})

        assert source['status']['last_job']['status'] == 'Finished'
        assert source['status']['last_job']['stats']['added'] == 10

        # Re-run the harvester
        job_dict = get_action('harvest_job_create')(
            context, {'source_id': source['id']})
        job_obj = HarvestJob.get(job_dict['id'])
        harvester = queue.get_harvester(source['source_type'])
        with requests_mock.Mocker(real_http=True) as m:
            m.register_uri('GET', '/dhus/search?q', text=self.raw_results)
            lib.run_harvest_job(job_obj, harvester)
        source = harvest_source_show(context, {'id': 'scihub_test_harvester'})

        assert source['status']['last_job']['status'] == 'Finished'
        assert source['status']['last_job']['stats']['added'] == 0
        assert source['status']['last_job']['stats']['updated'] == 0

        # Re-run the harvester but force updates
        config_dict = {'source': 'esa_scihub',
                       'update_all': True,
                       'datasets_per_job': 10,
                       'timeout': 10,
                       'skip_raw': False}
        config = json.dumps(config_dict)
        source['config'] = config
        harvest_source_update(context, source)
        job_dict = get_action('harvest_job_create')(
            context, {'source_id': source['id']})
        job_obj = HarvestJob.get(job_dict['id'])
        harvester = queue.get_harvester(source['source_type'])
        with requests_mock.Mocker(real_http=True) as m:
            m.register_uri('GET', '/dhus/search?q', text=self.raw_results)
            lib.run_harvest_job(job_obj, harvester)
        source = harvest_source_show(context, {'id': 'scihub_test_harvester'})

        assert source['status']['last_job']['status'] == 'Finished'
        assert source['status']['last_job']['stats']['added'] == 0
        assert source['status']['last_job']['stats']['updated'] == 10

        # Verify that the org now has 10 datasets now
        org = helpers.call_action('organization_show', context, **{'id':
                                                                   'test_org'})
        assert org['package_count'] == 10
