"""
Tests for executor module
"""
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
import os.path
import logging
from tornado.concurrent import Future
from tornado.testing import AsyncHTTPTestCase, gen_test
from tornado.web import RequestHandler, Application
from tornado.httpclient import HTTPError
from scrapydd.executor import ProjectEggDownloader

LOGGER = logging.getLogger(__name__)


class HandlerBase(RequestHandler):
    def data_received(self, chunk):
        pass


# pylint: disable=too-few-public-methods
class FakeEggDownloader:
    def __init__(self, egg_filepath):
        self.egg_filepath = egg_filepath

    def download_egg_future(self, spider_id):
        LOGGER.info('download_egg_future, spider_id: %s', spider_id)
        ret_future = Future()
        ret_future.set_result(self.egg_filepath)
        return ret_future


# pylint: disable=arguments-differ
class DownloadEggHandler(HandlerBase):
    def get(self, job_id):
        if job_id == '404':
            return self.set_status(404, 'Not Found')

        with open('tests/test_project-1.0-py2.7.egg', 'rb') as f_egg:
            return self.write(f_egg.read())


class DownloadEgg404Handler(HandlerBase):
    def get(self, job_id):
        LOGGER.info('DownloadEgg404Handler.get, job_id: %s', job_id)
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
            yield target.download_egg_future(job_id)
            self.fail("No exception caught")
        except HTTPError as ex:
            self.assertEqual(404, ex.code)
