import ckan.plugins as plugins


class NextgeossharvestPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
