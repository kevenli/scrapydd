import json
from .base import SpiderPlugin, BooleanPluginParameter


class Plugin(SpiderPlugin):
    parameters = [
        BooleanPluginParameter('ENABLED', required=True, default_value=True)
    ]
    plugin_name = 'splitvariants'

    def validate(self, parameters):
        try:
            enabled = get_bool(parameters['ENABLED'])
        except (ValueError, KeyError):
            return False

        return True

    def execute(self, parameters):
        enabled = get_bool(parameters.get('ENABLED', True))
        ret_dict = [
            {
                'op': 'set_dict',
                'target': 'SPIDER_MIDDLEWARES',
                'key': 'scrapy_splitvariants.SplitVariantsMiddleware',
                'value': 100,
            },
            {
                'op': 'set_var',
                'var': 'SPLITVARIANTS_ENABLED',
                'value': enabled
            }
        ]
        return ret_dict


def get_bool(value):
    try:
        return bool(int(value))
    except ValueError:
        if value in ("True", "true"):
            return True
        if value in ("False", "false"):
            return False
        raise ValueError("Supported values for boolean settings "
                         "are 0/1, True/False, '0'/'1', "
                         "'True'/'False' and 'true'/'false'")


TEMPLATE = '''
try: SPIDER_MIDDLEWARES
except NameError: SPIDER_MIDDLEWARES = {}
SPIDER_MIDDLEWARES['scrapy_splitvariants.SplitVariantsMiddleware']= 100

SPLITVARIANTS_ENABLED = %(enabled)s
'''


def execute(settings):
    enabled = get_bool(settings.get('ENABLED', True))
    return TEMPLATE % {'enabled': enabled}
