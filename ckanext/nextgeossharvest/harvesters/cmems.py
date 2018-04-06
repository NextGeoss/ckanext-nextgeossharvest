import json
import logging
from datetime import timedelta, datetime

from sqlalchemy import desc

from ckan import model
from ckan.plugins.core import implements
from ckan.common import config


from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject

from ckanext.nextgeossharvest.lib.cmems_base import CMEMSBase
from ckanext.nextgeossharvest.lib.nextgeoss_base import NextGEOSSHarvester


class CMEMSHarvester(CMEMSBase,
                     NextGEOSSHarvester):
    '''
    A Harvester for CMEMS Products.
    '''
    implements(IHarvester)

    def info(self):
        return {
            'name': 'cmems',
            'title': 'CMEMS',
            'description': 'A Harvester for CMEMS Products'
            }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if config_obj.get('source') not in {'esa_scihub', 'esa_noa', 'esa_code'}:  # noqa: E501
                raise ValueError('source is required and must be either scihub or noa')  # noqa: E501
            if 'start_date' in config_obj:
                try:
                    if config_obj['start_date'] != 'YESTERDAY':
                        datetime.strptime(config_obj['start_date'],
                                          '%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError:
                    raise ValueError('start_date format must be 2018-01-01T00:00:00.000Z')  # noqa: E501
            if 'end_date' in config_obj:
                try:
                    if config_obj['end_date'] != 'TODAY':
                        datetime.strptime(config_obj['end_date'],
                                          '%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError:
                    raise ValueError('end_date format must be 2018-01-01T00:00:00.000Z')  # noqa: E501
            if 'timeout' in config_obj:
                timeout = config_obj['timeout']
                if not isinstance(timeout, int) and not timeout > 0:
                    raise ValueError('timeout must be a positive integer')
        except ValueError as e:
            raise e

        return config

    def fetch_stage(self, harvest_object):
        return True

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.gather')
        log.debug('CMEMS Harvester gather_stage for job: %r', harvest_job)

        self.job = harvest_job
        self._set_source_config(harvest_job.source.config)

        # get current objects out of db
        query = (model
                 .Session
                 .query(HarvestObject.guid, HarvestObject.package_id)
                 .filter(HarvestObject.current is True)
                 .filter(HarvestObject.harvest_source_id ==
                         harvest_job.source.id))

        guid_to_package_id = dict((res[0], res[1]) for res in query)
        current_guids = set(guid_to_package_id.keys())
        current_guids_in_harvest = set()

        last_object = model.Session.query(HarvestObject). \
            filter(HarvestObject.harvest_source_id == self.job.source_id,
                   HarvestObject.import_finished is not None). \
            order_by(desc(HarvestObject.import_finished)).limit(1)
        if last_object:
            try:
                last_object = last_object[0]
                restart_date = self._get_object_extra(last_object,
                                                      'restart_date', '*')
            except IndexError:
                restart_date = '*'
        else:
            restart_date = '*'
        log.debug('Restart date is {}'.format(restart_date))

        # if self.source_config['start_date'] == 'YESTERDAY':
        #     star_s = datetime.strftime(datetime.now()-timedelta(1), '%Y-%m-%d')
        # else:
        #     star_s = self.source_config['start_date']

        # if self.source_config['end_date'] == 'TODAY':
        #     end_s = datetime.today().strftime('%Y-%m-%d')
        # else:
        #     end_s = self.source_config['end_date']

        start_date = self.source_config.get('start_date', restart_date)
        end_date = self.source_config.get('end_date', 'NOW')
        date_range = '[{} TO {}]'.format(start_date, end_date)

        start_date_to_inc = datetime.strptime(star_s, '%Y-%m-%d')

        ids = self._get_metadata_create_objects(start_date,
                                                end_date,
                                                start_date_to_inc,
                                                self.job,
                                                current_guids,
                                                current_guids_in_harvest)

        return ids

    def import_stage(self, harvest_object):
        context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
        }

        log = logging.getLogger(__name__ + '.import')
        log.debug('Import stage for harvest object: %s', harvest_object.id)

        if not harvest_object:
            log.error('No harvest object received')
            return False

        self._set_source_config(harvest_object.source.config)
        status = self._get_object_extra(harvest_object, 'status')

        # Get the last harvested object (if any)
        previous_object = (model
                           .Session
                           .query(HarvestObject)
                           .filter(HarvestObject.guid == harvest_object.guid)
                           .filter(HarvestObject.current is True)
                           .first())

        if status == 'delete':
            # Delete package
            self._delete_dataset(self, harvest_object, context)
            return True

        self._create_package_dict(harvest_object, context, previous_object)

        model.Session.commit()

        return True
