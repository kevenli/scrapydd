import unittest
import os
from scrapydd.utils.plugin import desc

TEST_PACKAGE_PATH = os.path.join(os.path.dirname(__file__),
                                 '..',
                                 'test_plugin',
                                 'dist',
                                 'test_plugin-1.0-py3.6.egg')


class PluginTest(unittest.TestCase):
    def test_desc(self):
        desc(TEST_PACKAGE_PATH)
