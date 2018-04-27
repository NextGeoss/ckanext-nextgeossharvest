"""Tests for the CMEMS harvester."""
import json

from ckan import model
import ckan.tests.helpers as helpers
from ckan.logic import get_action

from ckanext.harvest.logic.action.create import harvest_source_create
from ckanext.harvest.logic.action.get import harvest_source_show
from ckanext.harvest import queue
from ckanext.harvest.tests import lib
from ckanext.harvest.model import HarvestJob


sst = {
    'org': {
        'name': 'sst_test_org',
        'url': 'http://example.com/sst'
    },
    'config': {
        'harvester_type': 'sst',
        'start_date': 'YESTERDAY',
        'end_date': 'TODAY',
        'timeout': 10
    }
}

sic_north = {
    'org': {
        'name': 'sic_north_test_org',
        'url': 'http://example.com/sic_north'
    },
    'config': {
        'harvester_type': 'sic_north',
        'start_date': 'YESTERDAY',
        'end_date': 'TODAY',
        'timeout': 10
    }
}

sic_south = {
    'org': {
        'name': 'sic_south_test_org',
        'url': 'http://example.com/sic_south'
    },
    'config': {
        'harvester_type': 'sic_south',
        'start_date': 'YESTERDAY',
        'end_date': 'TODAY',
        'timeout': 10
    }
}

ocn = {
    'org': {
        'name': 'ocn_test_org',
        'url': 'http://example.com/ocn'
    },
    'config': {
        'harvester_type': 'ocn',
        'start_date': '2018-01-01',
        'end_date': '2018-01-3',
        'timeout': 10
    }
}

test_auth = {
    'username': 'testuser',
    'password': 'testpass'
}


# We're simulating a run covering two days
def test_harvester(test_config=ocn, test_ftp_status='ok', expected=20,
                   private=True):
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

        org = test_config['org']
        owner_org = helpers.call_action('organization_create', context, **org)

        config_dict = test_config['config']
        config_dict['test_ftp_status'] = test_ftp_status
        config_dict['username'] = test_auth['username']
        config_dict['password'] = test_auth['password']
        config_dict['make_private'] = private
        config = json.dumps(config_dict)

        source = {
            'url': 'http://example.com/{}_test_harvester'.format(
                config_dict['harvester_type']),
            'name': '{}_test_harvester'.format(config_dict['harvester_type']),
            'owner_org': owner_org['id'],
            'source_type': 'cmems',
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
        assert source['status']['last_job']['stats']['added'] == expected

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
        assert org_response['package_count'] == expected
