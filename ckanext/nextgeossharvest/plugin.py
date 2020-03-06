import ckan.plugins as plugins
import shapely
import json
import ast
from ignore_list.ignore_list import IGNORE_LIST
import logging

log = logging.getLogger(__name__)


class NextgeossharvestPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IPackageController, inherit=True)

    def before_index(self, pkg_dict):
        """Expand extras if they're saved as a single string."""
        # dataset_type:
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
                    error_message = "Collection {} not found."
                    log.error(error_message.format(collection_id))
            else:
                error_message = "Collection ID not found in pkg_dict"
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
