import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.nextgeossharvest import helpers


class NextgeossharvestPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)