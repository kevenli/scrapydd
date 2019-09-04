import unittest
from subprocess import Popen, PIPE
import sys
import time

class RunnerTest(unittest.TestCase):
    def test_run(self):
        env = {}
        env['SCRAPY_EGG'] = 'tests/test_project-1.0-py2.7.egg'
        p = Popen([sys.executable, '-m', 'scrapydd.utils.runner', 'list'], env=env, stdout=PIPE)
        while p.poll() == None:
            time.sleep(1)
        stdout, stderr = p.communicate()
        output = stdout
        self.assertEqual(output.strip().split(), '''error_spider
fail_spider
log_spider
success_spider
warning_spider'''.split())

    def test_extract_settings(self):
        env = {}
        env['SCRAPY_EGG'] = 'tests/test_project-1.0-py2.7.egg'
        env['SCRAPY_EXTRA_SETTINGS_MODULE'] = 'tests.utils.testsettings'
        p = Popen([sys.executable, '-m', 'scrapydd.utils.runner', 'settings', '--get', 'SOME_SETTING'], env=env, stdout=PIPE)
        while p.poll() == None:
            time.sleep(1)
        stdout, stderr = p.communicate()
        output = stdout
        self.assertEqual(output.strip(), 'TestModule')

    def test_project_settings(self):
        env = {}
        env['SCRAPY_EGG'] = 'tests/test_project-1.0-py2.7.egg'
        p = Popen([sys.executable, '-m', 'scrapydd.utils.runner', 'settings', '--get', 'SOME_SETTING'], env=env, stdout=PIPE)
        while p.poll() == None:
            time.sleep(1)
        stdout, stderr = p.communicate()
        output = stdout
        self.assertEqual(output.strip(), '1')
