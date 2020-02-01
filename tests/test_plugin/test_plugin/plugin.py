import json


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
                'default_value': 'True'
            }
        }
    })


def execute(settings):
    enabled = get_bool(settings.get('ENABLED', True))
    if enabled:
        return '''
try: SPIDER_MIDDLEWARES
except NameError: SPIDER_MIDDLEWARES = {}
SPIDER_MIDDLEWARES['scrapy_splitvariants.SplitVariantsMiddleware']= 100

SPLITVARIANTS_ENABLED = True
        '''
    else:
        return ''
