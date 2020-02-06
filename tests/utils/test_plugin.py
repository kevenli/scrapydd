import unittest
import os
import shutil
import json
from setuptools import Distribution, find_packages
from scrapydd.utils.plugin import desc, perform, list_, load_distribution

from contextlib import contextmanager

@contextmanager
def cd(path):
    old_dir = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(old_dir)

TEST_PACKAGE_PATH = os.path.join(os.path.dirname(__file__),
                                 '..',
                                 'test_plugin',
                                 'dist',
                                 'test_plugin-1.0-py3.6.egg')


class PluginTest(unittest.TestCase):
    def setUp(self):
        import sys
        if TEST_PLUGIN_EGG_PATH in sys.path:
            sys.path.remove(TEST_PLUGIN_EGG_PATH)

    def test_desc(self):
        egg_path = TEST_PLUGIN_EGG_PATH
        desc(egg_path)

    def test_perform(self):
        base_module = 'tests.utils.none_settings'
        input_file = 'input.json'
        output_file = 'generatexxx.py'
        if os.path.exists(input_file):
            os.remove(input_file)
        if os.path.exists(output_file):
            os.remove(output_file)
        with open(input_file, 'w') as f_input:
            f_input.write(json.dumps({
                'splitvariants': {
                    'ENABLED': True
                }
            }))

        perform(base_module, input_file=input_file, output_file=output_file,
                eggs=[TEST_PLUGIN_EGG_PATH])

        perform_output = open(output_file, 'r').read().splitlines()
        print(perform_output)
        self.assertTrue('from %s import *' % base_module in perform_output)

    @unittest.skip
    def test_list(self):
        plugin_list = list_()
        self.assertNotIn('splitvariants', plugin_list)
        d = load_distribution(TEST_PLUGIN_EGG_PATH)
        d.activate()
        plugin_list = list_()
        self.assertIn('splitvariants', plugin_list)



OUTPUT_DIR = 'output'
def build_sample_plugin():
    plugin_src_dir = os.path.join(os.path.dirname(__file__),
                                  '../../samples/scrapy-splitvariants-plugin')
    with cd(plugin_src_dir):
        shutil.rmtree('dist')
        d = Distribution(dict(
            version='1.0',
            name='scrapy_splitvariants_plugin',
            description='scrapy_splitvariants_plugin',
            packages=find_packages(exclude=['tests', 'tests.*']),
            entry_points={
                'scrapydd.spliderplugin': [
                    'splitvariants = scrapy_splitvariants_plugin.plugin:Plugin',
                ],
            },
            install_requires=[
                'scrapy',
                'scrapy-splitvariants'
            ],
            zip_safe=True,
        ))
        d.script_name = 'setup.py'
        d.script_args = ['--quiet', 'clean', 'bdist_egg']
        d.parse_command_line()
        d.run_commands()

        egg_name = os.listdir('dist')[0]
        return os.path.abspath(os.path.join('dist', egg_name))


TEST_PLUGIN_EGG_PATH = build_sample_plugin()
