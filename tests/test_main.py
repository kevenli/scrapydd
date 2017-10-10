from unittest import TestCase
from scrapydd.main import *
from scrapydd.config import Config
from tornado.testing import AsyncHTTPTestCase
from poster.encode import multipart_encode
from scrapydd.stream import MultipartRequestBodyProducer
from poster.streaminghttp import register_openers
register_openers()
import sys

class MainTest(AsyncHTTPTestCase):
    def get_app(self):
        #init_logging(Config())
        return make_app(None, None, None)

    def test_logging_init(self):
        self.skipTest('no logging init')
        init_logging(Config())

    def test_MainHandler(self):
        response = self.fetch('/')
        self.assertEqual(200, response.code)

    def test_UploadProject_post(self):
        os.environ['ASYNC_TEST_TIMEOUT'] = '30'
        post_data = {}
        post_data['egg'] = open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = 'test_project'
        post_data['version'] = '1.0'

        datagen, headers = multipart_encode(post_data)
        databuffer = ''.join(datagen)
        response = self.fetch('/addversion.json', method='POST', headers=headers, body=databuffer)

        self.assertEqual(200, response.code)

    def test_UploadProject_get(self):
        response = self.fetch('/addversion.json')
        self.assertEqual(200, response.code)