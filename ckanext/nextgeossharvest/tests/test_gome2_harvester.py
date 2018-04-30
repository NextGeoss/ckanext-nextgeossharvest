# -*- coding: utf-8 -*-
"""Tests for the CMEMS harvester."""

import json

from parameterized import parameterized

from ckan import model
import ckan.tests.helpers as helpers
from ckan.logic import get_action

from ckanext.harvest.logic.action.create import harvest_source_create
from ckanext.harvest.logic.action.get import harvest_source_show
from ckanext.harvest import queue
from ckanext.harvest.tests import lib
from ckanext.harvest.model import HarvestJob


config_1 = {
    'start_date': '2018-03-01',
    'end_date': '2018-04-30',
    'make_private': False
}

config_2 = {
    'start_date': '2005-03-01',
    'end_date': '2005-04-30',
    'make_private': False
}

config_3 = {
    'start_date': '2017-03-01',
    'end_date': '2017-03-10',
    'make_private': False
}

o3_and_tropno2 = {
    'start_date': '2007-09-01',
    'end_date': '2007-09-01',
    'make_private': False
}


@parameterized([
    # Run for one-day interval
    (config_1, 0),
    (config_2, 0),
    (config_3, 50),
    (o3_and_tropno2, 2)
])
def test_harvester(test_config, expected_count):
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

    org = {'name': 'gome2_test_org',
           'url': 'http://example.com/gome2'}
    owner_org = helpers.call_action('organization_create', context, **org)
    config = json.dumps(test_config)

    source = {
        'url': 'http://example.com/gome2_test_harvester',
        'name': 'gome2_test_harvester',
        'owner_org': owner_org['id'],
        'source_type': 'gome2',
        'config': config
    }
    harvest_source_create(context, source)
    source = harvest_source_show(context, {'id': source['name']})

    job_dict = get_action('harvest_job_create')(
        context, {'source_id': source['id']})
    job_obj = HarvestJob.get(job_dict['id'])

    harvester = queue.get_harvester(source['source_type'])

    lib.run_harvest_job(job_obj, harvester)

    source = harvest_source_show(context, {'id': source['name']})
    assert source['status']['last_job']['status'] == 'Finished'
    assert source['status']['last_job']['stats']['added'] == expected_count

    # Re-run the harvester without forcing updates
    job_dict = get_action('harvest_job_create')(
        context, {'source_id': source['id']})
    job_obj = HarvestJob.get(job_dict['id'])

    harvester = queue.get_harvester(source['source_type'])

    lib.run_harvest_job(job_obj, harvester)

    source = harvest_source_show(context, {'id': source['name']})

    assert source['status']['last_job']['status'] == 'Finished'
    assert source['status']['last_job']['stats']['added'] == 0
    assert source['status']['last_job']['stats']['updated'] == 0

    # Verify that the org has the expected number of datasets now
    org_response = helpers.call_action('organization_show', context,
                                       **{'id': org['name']})
    assert org_response['package_count'] == expected_count
