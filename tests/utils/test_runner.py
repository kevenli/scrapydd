import unittest
from subprocess import Popen, PIPE
import sys
import time
import json


class RunnerTest(unittest.TestCase):
    def test_run(self):
        env = {}
        env['SCRAPY_EGG'] = 'tests/test_project-1.0-py2.7.egg'
        p = Popen([sys.executable, '-m', 'scrapydd.utils.runner', 'list'], env=env, stdout=PIPE, stderr=PIPE)
        while p.poll() is None:
            time.sleep(1)
        stdout, stderr = p.communicate()
        output = stdout
        self.assertEqual(output.strip().split(), b'''error_spider
fail_spider
log_spider
sina_news
success_spider
warning_spider'''.split())

    def test_extract_settings(self):
        env = {}
        env['SCRAPY_EGG'] = 'tests/test_project-1.0-py2.7.egg'
        env['SCRAPY_SETTINGS_MODULE'] = 'tests.utils.testsettings'
        p = Popen([sys.executable, '-m', 'scrapydd.utils.runner', 'settings', '--get', 'SOME_SETTING'],
                  env=env, stdout=PIPE)
        while p.poll() is None:
            time.sleep(1)
        stdout, stderr = p.communicate()
        output = stdout
        self.assertEqual(output.strip(), b'TestModule')

    def test_project_settings(self):
        env = {}
        env['SCRAPY_EGG'] = 'tests/test_project-1.0-py2.7.egg'
        p = Popen([sys.executable, '-m', 'scrapydd.utils.runner', 'settings', '--get', 'SOME_SETTING'],
                  env=env, stdout=PIPE)
        while p.poll() is None:
            time.sleep(1)
        stdout, stderr = p.communicate()
        output = stdout
        self.assertEqual(output.strip(), b'1')

    def test_settings_add_middleware(self):
        env = {}
        env['SCRAPY_EGG'] = 'tests/test_project-1.0-py2.7.egg'
        p = Popen([sys.executable, '-m', 'scrapydd.utils.runner', 'settings', '--get', 'SPIDER_MIDDLEWARES'],
                  env=env, stdout=PIPE)
        while p.poll() is None:
            time.sleep(1)
        stdout, stderr = p.communicate()
        settings_spider_middlewares = json.loads(stdout)
        self.assertEqual(1, len(settings_spider_middlewares))
        self.assertEqual(543, settings_spider_middlewares['test_project.middlewares.MyCustomSpiderMiddleware'])

    def test_extract_settings_add_middleware(self):
        env = {}
        env['SCRAPY_EGG'] = 'tests/test_project-1.0-py2.7.egg'
        env['SCRAPY_SETTINGS_MODULE'] = 'tests.utils.testsettings'
        p = Popen([sys.executable, '-m', 'scrapydd.utils.runner', 'settings', '--get', 'SPIDER_MIDDLEWARES'],
                  env=env, stdout=PIPE)
        while p.poll() is None:
            time.sleep(1)
        stdout, stderr = p.communicate()
        settings_spider_middlewares = json.loads(stdout)
        self.assertEqual(2, len(settings_spider_middlewares))
        self.assertEqual(543, settings_spider_middlewares['test_project.middlewares.MyCustomSpiderMiddleware'])
        self.assertEqual(300, settings_spider_middlewares['scrapy.spidermiddlewares.depth.DepthMiddleware'])


