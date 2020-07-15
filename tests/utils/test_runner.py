import unittest
from subprocess import Popen, PIPE
import sys
import time
import json
import os
from io import StringIO


TEST_PROJECT_EGG = 'tests/test_project-1.0-py2.7.egg'


class RunnerTest(unittest.TestCase):
    def test_run(self):
        env = os.environ.copy()
        env['SCRAPY_EGG'] = TEST_PROJECT_EGG
        p = Popen([sys.executable, '-m', 'scrapydd.utils.runner', 'list'],
                  env=env, stdout=PIPE, stderr=PIPE)
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
        env['SCRAPY_SETTINGS_MODULE'] = 'tests.utils.somesettings'
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
        env['SCRAPY_SETTINGS_MODULE'] = 'tests.utils.somesettings'
        p = Popen([sys.executable, '-m', 'scrapydd.utils.runner', 'settings', '--get', 'SPIDER_MIDDLEWARES'],
                  env=env, stdout=PIPE)
        while p.poll() is None:
            time.sleep(1)
        stdout, stderr = p.communicate()
        settings_spider_middlewares = json.loads(stdout)
        self.assertEqual(2, len(settings_spider_middlewares))
        self.assertEqual(543, settings_spider_middlewares['test_project.middlewares.MyCustomSpiderMiddleware'])
        self.assertEqual(300, settings_spider_middlewares['scrapy.spidermiddlewares.depth.DepthMiddleware'])


class RunnerModuleTest(unittest.TestCase):
    def get_settings_in_process(self, settings, settings_key):
        import optparse
        parser = optparse.OptionParser()

        from scrapy.commands.settings import Command
        from scrapy.crawler import CrawlerProcess

        command = Command()
        command.settings = settings
        command.add_options(parser)
        command.crawler_process = CrawlerProcess(settings)
        opts, args = parser.parse_args(['scrapy', 'settings', '--get',
                                        'BOT_NAME'])
        with Capturing() as output:
            command.run(args, opts)

        return output

    @unittest.skip
    def test_modify_settings_inline(self):
        from scrapydd.utils import runner
        settings = runner.activate_project(TEST_PROJECT_EGG)
        settings.set('BOT_NAME', 'test')

        self.assertEqual(self.get_settings_in_process(settings, 'BOT_NAME'),
                         ['test'])


class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout
