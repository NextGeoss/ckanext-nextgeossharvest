import logging

from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester

class VitoHarvester(SingletonPlugin):

    implements(IHarvester)

    def info(self):
        '''
        Harvesting implementations must provide this method, which will return
        a dictionary containing different descriptors of the harvester. The
        returned dictionary should contain:

        * name: machine-readable name. This will be the value stored in the
          database, and the one used by ckanext-harvest to call the appropiate
          harvester.
        * title: human-readable name. This will appear in the form's select box
          in the WUI.
        * description: a small description of what the harvester does. This
          will appear on the form as a guidance to the user.

        A complete example may be::

            {
                'name': 'csw',
                'title': 'CSW Server',
                'description': 'A server that implements OGC's Catalog Service
                                for the Web (CSW) standard'
            }

        :returns: A dictionary with the harvester descriptors
        '''

        return {
            'name': 'VITO',
            'title': 'Harvester for VITO imagiery',
            'description': 'Harvesting data from VITO'
        }

    def validate_config(self, config):
        '''

        [optional]

        Harvesters can provide this method to validate the configuration
        entered in the form. It should return a single string, which will be
        stored in the database.  Exceptions raised will be shown in the form's
        error messages.

        :param harvest_object_id: Config string coming from the form
        :returns: A string with the validated configuration options
        '''
        pass

    def get_original_url(self, harvest_object_id):
        '''

        [optional]

        This optional but very recommended method allows harvesters to return
        the URL to the original remote document, given a Harvest Object id.
        Note that getting the harvest object you have access to its guid as
        well as the object source, which has the URL.
        This URL will be used on error reports to help publishers link to the
        original document that has the errors. If this method is not provided
        or no URL is returned, only a link to the local copy of the remote
        document will be shown.
        document will be shown.

        Examples:
            * For a CKAN record: http://{ckan-instance}/api/rest/{guid}
            * For a WAF record: http://{waf-root}/{file-name}
            * For a CSW record: http://{csw-server}/?Request=GetElementById&Id={guid}&...

        :param harvest_object_id: HarvestObject id
        :returns: A string with the URL to the original document
        '''
        obj = model.Session.query(HarvestObject). \
            filter(HarvestObject.id == harvest_object_id). \
            first()
        if not obj:
            return None

        return obj.source.url


    def gather_stage(self, harvest_job):
        '''
        The gather stage will receive a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
              stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database, specifying
              the guid and a reference to its job. The HarvestObjects need a
              reference date with the last modified date for the resource, this
              may need to be set in a different stage depending on the type of
              source.
            - creating and storing any suitable HarvestGatherErrors that may
              occur.
            - returning a list with all the ids of the created HarvestObjects.
            - to abort the harvest, create a HarvestGatherError and raise an
              exception. Any created HarvestObjects will be deleted.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        '''

        log = logging.getLogger(__name__ + 'VITO.gather')
        log.debug('VITOHarvester gater_stage for job: %r', harvest_job)
        self.harvest_job = harvest_job

        # Get source url
        url = harvest_job.source.url

        self._set_source_config(harvest_job.source.config)

        # Get contents
        try:
            content = self._get_content_as_unicode(url)
        except Exception, e:
            self._save_gather_error('Unable to get content for URL: %s: %r' % \
                                    (url, e), harvest_job)
            return None

        existing_object = model.Session.query(HarvestObject.guid, HarvestObject.package_id). \
            filter(HarvestObject.current == True). \
            filter(HarvestObject.harvest_source_id == harvest_job.source.id). \
            first()
        print url

        log.info(url)
        return harvest_job

    def fetch_stage(self, harvest_object):
        pass

    def import_stage(self, harvest_object):
        pass