import json
from .base import SpiderPlugin


class TestPlugin(SpiderPlugin):
    def desc(self):
        return desc()

    def execute(self, parameters):
        return execute(parameters)

    def validate(self, parameters):
        try:
            enabled = get_bool(parameters.get('ENABLED', True))
        except ValueError:
            return False

        return True


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


def desc():
    return json.dumps({
        'name': 'test_plugin',
        'parameters': {
            'ENABLED': {
                'type': 'bool',
                'required': True,
                'default_value': True
            }
        }
    })


TEMPLATE = '''
try: SPIDER_MIDDLEWARES
except NameError: SPIDER_MIDDLEWARES = {}
SPIDER_MIDDLEWARES['scrapy_splitvariants.SplitVariantsMiddleware']= 100

SPLITVARIANTS_ENABLED = %(enabled)s
'''


def execute(settings):
    enabled = get_bool(settings.get('ENABLED', True))
    return TEMPLATE % {'enabled': enabled}
