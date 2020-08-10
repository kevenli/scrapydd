import sys
import asyncio
import unittest
from tornado.testing import AsyncHTTPTestCase, gen_test
from tornado.web import Application
from scrapydd.handlers.api import apply
from scrapydd.nodes import NodeManager
from scrapydd.models import init_database
from scrapydd.config import Config


class NodesHandlerTest(AsyncHTTPTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    def get_app(self) -> Application:
        config = Config(values={'database_url': 'sqlite:///test.db'})
        init_database(config)
        app = Application()
        app.settings['node_manager'] = NodeManager(None)
        apply(app)
        return app

    def test_post(self):
        res = self.fetch('/v1/nodes', method='POST', body='')
        self.assertEqual(200, res.code)

