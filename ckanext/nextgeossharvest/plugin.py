import ckan.plugins as plugins


class NextgeossharvestPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer, inherit=True)
    plugins.implements(plugins.IPackageController, inherit=True)

    def before_index(self, pkg_dict):
        """Expand extras if they're saved as a single string."""
        # If dataset_type is:
        # 'dataset' means that a dataset is being indexed
        # 'harvest' means that a harvest job is being indexed
        dataset_type =  pkg_dict.get('dataset_type', None)
        if 'dataset' == dataset_type:
            dataset_extra = pkg_dict.pop("dataset_extra", None)
            if dataset_extra:
                pkg_dict.update(convert_dataset_extra(dataset_extra))
            pkg_dict.pop("extras_dataset_extra", None)

            collection_id = pkg_dict.get('collection_id', None)
            if collection_id:
                try:
                    fields_list = IGNORE_LIST[collection_id]
                    pkg_dict = remove_fields_from_index(pkg_dict, fields_list)
                except:
                    pass
            else:
                error_message = ("Couldn't check ignore list in before_index."
                                 " Collection ID not found in package metadata.")
                log.error(error_message)

        return pkg_dict


def convert_dataset_extra(dataset_extra_string):
    """Convert the dataset_extra string into indexable extras."""
    extras = ast.literal_eval(dataset_extra_string)

    return [(extra["key"], extra["value"]) for extra in extras]


def remove_from_dict(pkg_dict, key):
    key_exists = pkg_dict.get(key, None)
    if key_exists is not None:
        pkg_dict.pop(key, None)
    return pkg_dict


def remove_fields_from_index(pkg_dict, field_list):

    extras_field_template = "extras_{}"
    for field in field_list:
        pkg_dict = remove_from_dict(pkg_dict, field)
        extras_field = extras_field_template.format(field)
        pkg_dict = remove_from_dict(pkg_dict, extras_field)

    return pkg_dict
