import os

from tornado.testing import AsyncTestCase, gen_test

from scrapydd.spiderplugin import SpiderPluginManager
from scrapydd.config import Config
from scrapydd.models import init_database, SpiderPluginParameterDatatype


TETS_PACKAGE_PATH = os.path.join(os.path.dirname(__file__), 'test_plugin',
                                 'dist',
                                 'test_plugin-1.0-py3.6.egg')


class SpiderPluginManagerTest(AsyncTestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['ASYNC_TEST_TIMEOUT'] = '500'
        if os.path.exists('test.db'):
            os.remove('test.db')
        config = Config(values={'database_url': 'sqlite:///test.db'})
        init_database(config)

    @gen_test(timeout=120)
    def test_add_sys_plugin(self):
        target = SpiderPluginManager()
        plugin_name = 'test_plugin'
        f_egg = open(TETS_PACKAGE_PATH, 'rb')
        actual = yield target.get_plugin_info(f_egg, plugin_name)
        self.assertEqual('test_plugin', actual.get('name'))
        self.assertIsNotNone(actual.get('parameters'))
        parameters = actual.get('parameters')
        self.assertEqual(1, len(parameters.keys()))
        self.assertTrue('ENABLED' in parameters)
        enabled_parameter = parameters['ENABLED']
        self.assertEqual('bool', enabled_parameter['type'])
        self.assertEqual(True, enabled_parameter['required'])

    @gen_test(timeout=120)
    def test_plugin_execute(self):
        target = SpiderPluginManager()
        plugin_name = 'test_plugin'
        f_egg = open(TETS_PACKAGE_PATH, 'rb')
        yield target.add_sys_plugin(f_egg, plugin_name)
        saved = target.get_plugin(plugin_name)
        self.assertIsNotNone(saved)
        self.assertEqual(saved.name, plugin_name)
        self.assertEqual(len(saved.parameters), 1)
        parameter = saved.parameters[0]
        self.assertEqual(parameter.key, 'ENABLED')
        self.assertEqual(parameter.datatype,
                         SpiderPluginParameterDatatype.bool)
        self.assertEqual(parameter.required, True)
        self.assertEqual(parameter.default_value, 'True')
