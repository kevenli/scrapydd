import unittest
import logging
import os.path
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.testing import AsyncHTTPTestCase, gen_test
from tornado.web import RequestHandler, Application
from tornado.httpclient import HTTPError
from scrapydd.agent import AgentConfig
from scrapydd.executor import SpiderTask, TaskExecutor, ProjectEggDownloader


@unittest.skip
class TaskExecutorTests(unittest.TestCase):
    def test_execute(self):
        task =SpiderTask()
        task.spider_name
        config = AgentConfig()


class FakeEggDownloader(object):
    def __init__(self, egg_filepath):
        self.egg_filepath = egg_filepath

    def download_egg_future(self, spider_id):
        ret_future = Future()
        ret_future.set_result(self.egg_filepath)
        return ret_future


class DownloadEggHandler(RequestHandler):
    def get(self, job_id):
        if job_id == '404':
            return self.set_status(404, 'Not Found')

        with open('tests/test_project-1.0-py2.7.egg', 'rb') as f:
            self.write(f.read())


class DownloadEgg404Handler(RequestHandler):
    def get(self, job_id):
        self.set_status(404, 'Not Found')


class ProjectEggDownloaderTest(AsyncHTTPTestCase):
    def get_app(self):
        return Application([
            (r"/jobs/(\w+)/egg", DownloadEggHandler)
        ])

    @gen_test
    def test_download_egg_future(self):
        job_id = 'xxx'
        target = ProjectEggDownloader(self.get_url('/'))
        download_path = yield target.download_egg_future(job_id)
        self.assertIsNotNone(download_path)
        self.assertTrue(os.path.exists(download_path))
        self.assertTrue(os.path.exists(target.download_path))

        target_download_path = target.download_path
        del target
        self.assertFalse(os.path.exists(download_path))
        self.assertFalse(os.path.exists(target_download_path))

    @gen_test
    def test_download_egg_future_fail(self):
        job_id = '404'
        target = ProjectEggDownloader(self.get_url('/some_error_request/'))
        try:
            download_path = yield target.download_egg_future(job_id)
            self.fail("No exception caught")
        except HTTPError as e:
            self.assertEqual(404, e.code)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    task = SpiderTask()
    task.spider_id = 1
    task.project_name = 'test_project'
    task.spider_name = 'success_spider'
    task.project_version = '1.0'
    task.id = 1
    task.spider_parameters = {}

    def execute(future=None):
        executor = TaskExecutor(task, FakeEggDownloader('test_project/dist/test_project-1.0-py2.7.egg'), '/work')
        executor.execute().add_done_callback(execute)
    #period = PeriodicCallback(execute, 1000)
    #period.start()
    execute()
    IOLoop.current().start()