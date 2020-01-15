from tornado.testing import gen_test, AsyncTestCase
from tornado.ioloop import IOLoop
from scrapydd.workspace import ProjectWorkspace, VenvRunner, SpiderSetting, DockerRunner
from scrapydd.exceptions import ProcessFailed
import tempfile
import os
from unittest import TestCase, skip
import json

test_project_file = os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg')


class ProjectWorkspaceTest(AsyncTestCase):
    @gen_test(timeout=200)
    def test_init(self):
        target = ProjectWorkspace('test_project')

        yield target.init()
        self.assertTrue(os.path.exists(target.python))
        self.assertTrue(os.path.exists(target.pip))

        self.assertTrue(file_is_in_dir(tempfile.gettempdir(), target.python))

    @gen_test(timeout=200)
    def test_init_after_init(self):
        target = ProjectWorkspace('test_project')

        yield target.init()
        yield target.init()
        self.assertTrue(os.path.exists(target.python))
        self.assertTrue(os.path.exists(target.pip))

    @gen_test(timeout=200)
    def test_init_kill(self):
        target = ProjectWorkspace('test_project')

        IOLoop.current().call_later(1, target.kill_process)

        try:
            yield target.init()
            self.fail('Exception not caught')
        except ProcessFailed:
            pass
        except Exception as e:
            self.fail('ProcessFailed exception not caught. %s' % e)
        self.assertEqual(len(target.processes), 0)

    def test_find_requirements(self):
        target = ProjectWorkspace('test_project')
        target.put_egg(open(test_project_file, 'rb'), '1.0')
        self.assertEqual(target.find_project_requirements(), ['scrapy'])

    @gen_test(timeout=200)
    def test_install_requirements(self):
        target = ProjectWorkspace('test_project')
        target.put_egg(open(test_project_file, 'rb'), '1.0')
        yield target.init()
        yield target.install_requirements()


    @gen_test(timeout=200)
    def test_spider_list(self):
        target = ProjectWorkspace('test_project')
        yield target.init()
        target.put_egg(open(test_project_file, 'rb'), '1.0')
        yield target.install_requirements()
        spider_list = yield target.spider_list()
        self.assertEqual(['error_spider', 'fail_spider', 'log_spider', 'sina_news', 'success_spider',
                          'warning_spider'],
                         spider_list)


def file_is_in_dir(dir, file):
    if os.path.dirname(file) == dir:
        return True

    parent_dir = os.path.dirname(file)
    if parent_dir == file:
        return False

    return file_is_in_dir(dir, parent_dir)


class VenvRunnerTest(AsyncTestCase):
    @gen_test(timeout=200)
    def test_list(self):
        eggf = open(test_project_file, 'rb')
        target = VenvRunner(eggf)
        spider_list = yield target.list()
        self.assertEqual(['error_spider', 'fail_spider', 'log_spider', 'sina_news', 'success_spider',
                          'warning_spider'],
                         spider_list)

    @gen_test(timeout=200)
    def test_crawl(self):
        eggf = open(test_project_file, 'rb')
        spider_settings = SpiderSetting('fail_spider')
        target = VenvRunner(eggf)
        ret = yield target.crawl(spider_settings)
        self.assertIsNotNone(ret)
        self.assertEqual(0, ret.ret_code)
        self.assertIsNotNone(ret.items_file)
        self.assertTrue(os.path.exists(ret.items_file))
        self.assertIsNotNone(ret.crawl_logfile)
        self.assertTrue(os.path.exists(ret.crawl_logfile))

    @gen_test(timeout=200)
    def test_crawl_overwrite_setting(self):
        eggf = open(test_project_file, 'rb')
        spider_settings = SpiderSetting('log_spider', spider_parameters={'SOME_SETTING': '2'})
        target = VenvRunner(eggf)
        ret = yield target.crawl(spider_settings)
        self.assertIsNotNone(ret)
        self.assertEqual(0, ret.ret_code)
        self.assertIsNotNone(ret.items_file)
        self.assertTrue(os.path.exists(ret.items_file))
        self.assertIsNotNone(ret.crawl_logfile)
        self.assertTrue(os.path.exists(ret.crawl_logfile))
        with open(ret.crawl_logfile, 'r') as f:
            crawl_log = f.read()
        self.assertTrue('SOME_SETTING: 2' in crawl_log)

    @gen_test(timeout=200)
    def test_crawl_process_fail(self):
        eggf = open(test_project_file, 'rb')
        spider_settings = SpiderSetting('NO_EXIST_SPIDER')
        target = VenvRunner(eggf)
        try:
            ret = yield target.crawl(spider_settings)
            self.fail('Did not caught ProcessFail exception.')
        except ProcessFailed as e:
            self.assertIsNotNone(e.err_output)
            self.assertTrue("KeyError: 'Spider not found: NO_EXIST_SPIDER'" in e.err_output)

    @gen_test(timeout=200)
    def test_clear(self):
        eggf = open(test_project_file, 'rb')
        spider_settings = SpiderSetting('fail_spider')
        target = VenvRunner(eggf)
        ret = yield target.crawl(spider_settings)
        self.assertTrue(os.path.exists(ret.items_file))
        self.assertTrue(os.path.exists(ret.crawl_logfile))

        target.clear()
        self.assertFalse(os.path.exists(target._work_dir))
        self.assertFalse(os.path.exists(ret.items_file))
        self.assertFalse(os.path.exists(ret.crawl_logfile))

    @gen_test(timeout=200)
    def test_kill_crawl(self):
        eggf = open(test_project_file, 'rb')
        spider_settings = SpiderSetting('fail_spider')
        target = VenvRunner(eggf)
        target.image = 'scrapydd:develop'
        future = target.crawl(spider_settings)
        target.kill()
        try:
            ret = yield future
            self.fail("Didnot caught ProcessFailed exception")
        except ProcessFailed:
            pass

    @gen_test(timeout=200)
    def test_kill_list(self):
        eggf = open(test_project_file, 'rb')
        spider_settings = SpiderSetting('fail_spider')
        target = VenvRunner(eggf)
        target.image = 'scrapydd:develop'
        future = target.list()
        target.kill()
        try:
            ret = yield future
            self.fail("Did not caught the ProcessFailed")
        except ProcessFailed:
            pass

    @skip
    @gen_test(timeout=200)
    def test_settings_module(self):
        eggf = open(test_project_file, 'rb')
        target = VenvRunner(eggf)
        ret = yield target.settings_module()
        self.assertEqual('test_project.settings', ret)


class DockerRunnerTest(AsyncTestCase):
    @gen_test(timeout=200)
    def test_list(self):
        eggf = open(test_project_file, 'rb')
        target = DockerRunner(eggf)
        target.image = 'scrapydd:develop'
        spider_list = yield target.list()
        self.assertEqual(['error_spider', 'fail_spider', 'log_spider', 'sina_news', 'success_spider',
                          'warning_spider'],
                         spider_list)

    @gen_test(timeout=200)
    def test_crawl(self):
        eggf = open(test_project_file, 'rb')
        spider_settings = SpiderSetting('fail_spider')
        target = DockerRunner(eggf)
        target.image = 'scrapydd:develop'
        ret = yield target.crawl(spider_settings)
        self.assertIsNotNone(ret)
        self.assertEqual(0, ret.ret_code)
        self.assertIsNotNone(ret.items_file)
        self.assertTrue(os.path.exists(ret.items_file))
        self.assertIsNotNone(ret.crawl_logfile)
        self.assertTrue(os.path.exists(ret.crawl_logfile))

    @gen_test(timeout=200)
    def test_crawl_overwrite_setting(self):
        eggf = open(test_project_file, 'rb')
        spider_settings = SpiderSetting('log_spider', spider_parameters={'SOME_SETTING': 'abc'})
        spider_settings.base_settings_module = 'test_project.settings'
        target = DockerRunner(eggf)
        target.image = 'scrapydd:develop'
        ret = yield target.crawl(spider_settings)
        self.assertIsNotNone(ret)
        self.assertEqual(0, ret.ret_code)
        self.assertIsNotNone(ret.items_file)
        self.assertTrue(os.path.exists(ret.items_file))
        self.assertIsNotNone(ret.crawl_logfile)
        self.assertTrue(os.path.exists(ret.crawl_logfile))
        with open(ret.crawl_logfile, 'r') as f:
            crawl_log = f.read()
        self.assertIn('SOME_SETTING: abc', crawl_log)

    @gen_test(timeout=200)
    def test_clear(self):
        eggf = open(test_project_file, 'rb')
        spider_settings = SpiderSetting('fail_spider')
        target = DockerRunner(eggf)
        target.image = 'scrapydd:develop'
        ret = yield target.crawl(spider_settings)
        self.assertTrue(os.path.exists(ret.items_file))
        self.assertTrue(os.path.exists(ret.crawl_logfile))

        target.clear()
        self.assertFalse(os.path.exists(target._work_dir))
        self.assertFalse(os.path.exists(ret.items_file))
        self.assertFalse(os.path.exists(ret.crawl_logfile))

    @gen_test(timeout=200)
    def test_settings_module(self):
        eggf = open(test_project_file, 'rb')
        target = DockerRunner(eggf)
        target.image = 'scrapydd:develop'
        ret = yield target.settings_module()
        self.assertIsNone(target._container)
        self.assertEqual('test_project.settings', ret)

    @gen_test(timeout=200)
    def test_kill_crawl(self):
        eggf = open(test_project_file, 'rb')
        spider_settings = SpiderSetting('fail_spider')
        target = DockerRunner(eggf)
        target.image = 'scrapydd:develop'
        future = target.crawl(spider_settings)
        target.kill()
        try:
            ret = yield future
            self.fail("Didnot caught ProcessFailed exception")
        except ProcessFailed:
            pass


    @gen_test(timeout=200)
    def test_kill_list(self):
        eggf = open(test_project_file, 'rb')
        spider_settings = SpiderSetting('fail_spider')
        target = DockerRunner(eggf)
        target.image = 'scrapydd:develop'
        future = target.list()
        target.kill()
        try:
            ret = yield future
            self.fail("Did not caught the ProcessFailed")
        except ProcessFailed:
            pass


class SpiderSettingsTest(TestCase):
    def test_to_json(self):
        spider_name = 'abc'
        target = SpiderSetting(spider_name)

        json_text = target.to_json()
        json_deserialized = json.loads(json_text)

        self.assertEqual(json_deserialized['spider_name'], spider_name)
        self.assertEqual(json_deserialized['project_name'], None)
        self.assertEqual(json_deserialized['extra_requirements'], [])
        self.assertEqual(json_deserialized['spider_parameters'], {})

    def test_from_json(self):
        spider_name = 'abc'
        json_text = '''
        {
            "spider_name": "abc",
            "project_name": "xyz",
            "extra_requirements": [
                "scrapy",
                "beautifulsoup4"
            ],
            "spider_parameters" : {
                "parameter_a" : "value_a",
                "parameter_b" : "value_b"
            }
        }
        '''
        target = SpiderSetting.from_json(json_text)
        self.assertEqual(target.spider_name, spider_name)
        self.assertEqual(target.project_name, 'xyz')
        self.assertEqual(target.extra_requirements, [
            "scrapy",
            "beautifulsoup4"
        ])
        self.assertEqual(target.spider_parameters, {
            'parameter_a': 'value_a',
            'parameter_b': 'value_b'
        })
