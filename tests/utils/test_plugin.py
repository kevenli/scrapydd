import unittest
import os
import shutil
from setuptools import Distribution, find_packages
from scrapydd.utils.plugin import desc

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
    def test_desc(self):
        egg_path = TEST_PLUGIN_EGG_PATH
        desc(egg_path)

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
        ))
        d.script_name = 'setup.py'
        d.script_args = ['--quiet', 'clean', 'bdist_egg']
        d.parse_command_line()
        d.run_commands()

        egg_name = os.listdir('dist')[0]
        return os.path.abspath(os.path.join('dist', egg_name))


TEST_PLUGIN_EGG_PATH = build_sample_plugin()
