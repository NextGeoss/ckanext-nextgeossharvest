import ckan.plugins as plugins
import shapely
import json
import ast

class NextgeossharvestPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IPackageController, inherit=True)

    def before_index(self, pkg_dict):
        """Expand extras if they're saved as a single string."""
        dataset_extra = pkg_dict.pop("dataset_extra", None)
        if dataset_extra:
            pkg_dict.update(convert_dataset_extra(dataset_extra))
        pkg_dict.pop("extras_dataset_extra", None)

        # Handle spatial indexing here since the string extras break
        # the spatial extension.
        #pkg_dict = remove_sentinel_fields_from_index(pkg_dict)

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


def remove_sentinel_fields_from_index(pkg_dict):
    field_list = [
        "FamilyName",
        "InstrumentName",
        "ProductClass",
        "uuid",
        "AcquisitionType",
        "noa_expiration_date",
        "Filename",
        "size",
        "thumbnail",
        "summary",
        "scihub_download_url",
        "scihub_product_url",
        "scihub_manifest_url",
        "scihub_thumbnail",
        "noa_download_url",
        "noa_product_url",
        "noa_manifest_url",
        "noa_thumbnail",
        "code_download_url",
        "code_product_url",
        "code_manifest_url",
        "code_thumbnail",
    ]

    for field in field_list:
        pkg_dict = remove_from_dict(pkg_dict, field)

    return pkg_dict
