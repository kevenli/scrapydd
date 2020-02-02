import os

from tornado.testing import AsyncTestCase, gen_test
from tornado import gen

from scrapydd.spiderplugin import SpiderPluginManager

class SpiderPluginManagerTest(AsyncTestCase):

    @gen_test(timeout=120)
    def test_add_sys_plugin(self):
        target = SpiderPluginManager()
        plugin_name = 'test_plugin'
        f_egg = open(os.path.join(os.path.dirname(__file__), plugin_name,
                                  'dist',
                                  'test_plugin-1.0-py3.6.egg'), 'rb')
        actual = yield target.get_plugin_info(f_egg, plugin_name)
        self.assertEqual('test_plugin', actual.get('name'))
        self.assertIsNotNone(actual.get('parameters'))
        parameters = actual.get('parameters')
        self.assertEqual(1, len(parameters.keys()))
        self.assertTrue('ENABLED' in parameters)
        enabled_parameter = parameters['ENABLED']
        self.assertEqual('bool', enabled_parameter['type'])
        self.assertEqual(True, enabled_parameter['required'])
