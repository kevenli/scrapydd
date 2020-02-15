import unittest
from scrapy_splitvariants_plugin.plugin import Plugin


class PluginTest(unittest.TestCase):
    def test_desc(self):
        target = Plugin()
        actual = target.desc()

        self.assertEqual('splitvariants', actual['name'])
        parameters = actual['parameters']
        self.assertEqual(len(parameters), 1)
        enabled_parameter = parameters['ENABLED']
        self.assertEqual('bool', enabled_parameter['type'])
        self.assertEqual(True, enabled_parameter['required'])
        self.assertEqual(True, enabled_parameter['default_value'])

    def test_validate(self):
        parameter_values = {'ENABLED':'True'}
        target = Plugin()
        ret = target.validate(parameter_values)
        self.assertTrue(ret)

    def test_validate_nokey(self):
        parameter_values = {}
        target = Plugin()
        ret = target.validate(parameter_values)
        self.assertFalse(ret)

    def test_validate_wrongtype(self):
        parameter_values = {
            'ENABLED': 'foo'
        }
        target = Plugin()
        ret = target.validate(parameter_values)
        self.assertFalse(ret)

    def test_execute(self):
        parameter_values = {
            'ENABLED': 'True'
        }
        target = Plugin()
        ret_ops = target.execute(parameter_values)
        self.assertEqual(2, len(ret_ops))
        ret_ops_1 = ret_ops[0]
        self.assertEqual(ret_ops_1['op'], 'set_dict')
        self.assertEqual(ret_ops_1['target'], 'SPIDER_MIDDLEWARES')
        self.assertEqual(ret_ops_1['key'],
                         'scrapy_splitvariants.SplitVariantsMiddleware')
        self.assertEqual(ret_ops_1['value'], 100)

        ret_ops_2 = ret_ops[1]
        self.assertEqual(ret_ops_2['op'], 'set_var')
        self.assertEqual(ret_ops_2['var'], 'SPLITVARIANTS_ENABLED')
        self.assertEqual(ret_ops_2['value'], True)
