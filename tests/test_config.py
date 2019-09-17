import unittest
import os
from scrapydd.config import Config
from six.moves.configparser import ConfigParser, SafeConfigParser, NoSectionError, NoOptionError

class ConfigTest(unittest.TestCase):
    def test_get(self):
        target = Config()
        self.assertEqual('0.0.0.0', target.get('bind_address'))
        
    def test_getint(self):
        target = Config()
        self.assertEqual(6800, target.getint('bind_port'))

    def test_getfloat(self):
        target = Config()
        self.assertEqual(6800, target.getfloat('bind_port'))

    def test_getboolean(self):
        target = Config(extra_sources=('tests/test.conf', ))
        self.assertEqual(False, target.getboolean('debug'))

    def test_get_from_env(self):
        target = Config()
        database_url = 'sqlite:///test.db'
        os.environ['SCRAPYDD_DATABASE_URL'] = 'sqlite:///test.db'
        self.assertEqual( database_url,target.get('database_url'))
        del os.environ['SCRAPYDD_DATABASE_URL']

    def test_get_int_from_env(self):
        target = Config()
        bind_port = '1'
        os.environ['SCRAPYDD_BIND_PORT'] = bind_port

        self.assertEqual(int(bind_port), target.getint('bind_port'))
        del os.environ['SCRAPYDD_BIND_PORT']

    def test_get_int_from_env(self):
        target = Config()
        debug = 'True'
        os.environ['SCRAPYDD_DEBUG'] = debug
        self.assertEqual(True, target.getboolean('debug'))
        del os.environ['SCRAPYDD_DEBUG']

    def test_get_bool_from_extrafiles(self):
        target = Config(extra_sources=('tests/test.conf',))
        self.assertEqual(False, target.getboolean('BoolSetting'))
        self.assertEqual(True, target.getboolean('BoolSettingTrue'))

    def test_get_from_values(self):
        target = Config(values={'SomeSetting': 'Value'})
        self.assertEqual('Value', target.get('SomeSetting'))

    def test_get_raise_no_option_error(self):
        target = Config()

        try:
            target.get('SomeNotExistOption')
            self.fail('No Error caught')
        except NoOptionError:
            pass

        v = target.get('SomeNotExistOption', 'DefaultValue')
        self.assertEqual(v, 'DefaultValue')
