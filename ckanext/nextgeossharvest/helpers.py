import ast

from ckan.common import config


def harvest_sorted_extras(package_extras, auto_clean=False, subs=None,
                          exclude=None):
    ''' Used for outputting package extras

    :param package_extras: the package extras
    :type package_extras: dict
    :param auto_clean: If true capitalize and replace -_ with spaces
    :type auto_clean: bool
    :param subs: substitutes to use instead of given keys
    :type subs: dict {'key': 'replacement'}
    :param exclude: keys to exclude
    :type exclude: list of strings
    '''

    # If exclude is not supplied use values defined in the config
    if not exclude:
        exclude = config.get('package_hide_extras', [])
    output = []
    for extra in sorted(package_extras, key=lambda x: x['key']):
        if extra.get('state') == 'deleted':
            continue
        extras_tmp = ast.literal_eval(extra['value'])

        for ext in extras_tmp:
            k, v = ext['key'], ext['value']
            if k in exclude:
                continue
            if subs and k in subs:
                k = subs[k]
            elif auto_clean:
                k = k.replace('_', ' ').replace('-', ' ').title()
            if isinstance(v, (list, tuple)):
                v = ", ".join(map(unicode, v))
            output.append((k, v))
    return output
