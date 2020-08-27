import sys
import asyncio
import unittest
import json
from urllib.parse import urlencode
from tornado.testing import AsyncHTTPTestCase, gen_test
from tornado.web import Application
from scrapydd.handlers.api import apply
from scrapydd.nodes import NodeManager
from scrapydd.project import ProjectManager
from scrapydd.models import init_database
from scrapydd.config import Config
from ..base import AppBuilder


class ApiTestBase(AppBuilder, AsyncHTTPTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    def post_json(self, path, **kwargs):
        headers = kwargs.setdefault('headers', {})
        kwargs['method'] = 'POST'
        headers['Content-Type'] = 'application/json'
        return self.fetch(path, **kwargs)


class ProjectsHandler(ApiTestBase):
    def test_post(self):
        name = 'ProjectsHandler.test_post'
        post_data = {'name': name}
        res = self.fetch('/v1/projects', method='POST',
                         body=urlencode(post_data))
        res_data = json.loads(res.body)
        self.assertEqual(200, res.code)
        self.assertIsNotNone(res_data['id'])
        self.assertEqual(res_data['name'], name)

    def test_post_no_name(self):
        name = 'ProjectsHandler.test_post'
        res = self.fetch('/v1/projects', method='POST',
                         body='')
        res_data = json.loads(res.body)
        self.assertEqual(400, res.code)
        self.assertEqual(res_data['errmsg'], 'Parameter name required.')

    def test_post_querystring(self):
        name = 'ProjectsHandler.test_post_querystring'
        post_data = {'name': name}
        res = self.fetch('/v1/projects?name='+name, method='POST',
                         body='')
        res_data = json.loads(res.body)
        self.assertEqual(200, res.code)
        self.assertIsNotNone(res_data['id'])
        self.assertEqual(res_data['name'], name)

    def test_post_json(self):
        name = 'ProjectsHandler.test_post_json'
        post_data = {'name': name}
        res = self.post_json('/v1/projects',
                         body=json.dumps(post_data))
        res_data = json.loads(res.body)
        self.assertEqual(200, res.code)
        self.assertIsNotNone(res_data['id'])
        self.assertEqual(res_data['name'], name)
