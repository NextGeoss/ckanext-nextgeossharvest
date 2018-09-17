import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.nextgeossharvest import helpers


class NextgeossharvestPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers, inherit=True)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')

    # ITemplateHelpers

    def get_helpers(self):
        return {
            'harvest_sorted_extras': helpers.harvest_sorted_extras,
        }
