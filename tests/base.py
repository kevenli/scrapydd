import os
from six import ensure_str, ensure_binary
from w3lib.http import basic_auth_header
from tornado.testing import AsyncHTTPTestCase
from tornado.concurrent import Future
from tornado.web import create_signed_value
from tornado.httputil import HTTPHeaders
from tornado.ioloop import IOLoop
from scrapydd.poster.encode import multipart_encode
from scrapydd.security import encrypt_password
from scrapydd.models import init_database, session_scope, User, Project, Spider
from scrapydd.config import Config
from scrapydd.schedule import SchedulerManager
from scrapydd.nodes import NodeManager
from scrapydd.main import make_app
from scrapydd.webhook import WebhookDaemon
from scrapydd.settting import SpiderSettingLoader
from scrapydd.storage import ProjectStorage
from scrapydd.workspace import CrawlResult
from scrapydd.project import ProjectManager


class TestRunnerStub:
    def __init__(self, eggf):
        pass

    def list(self):
        ret = ['error_spider', 'fail_spider', 'log_spider', 'success_spider', 'warning_spider']
        future = Future()
        future.set_result(ret)
        return future

    def crawl(self, settings):
        future = Future()
        future.set_result(CrawlResult(0))
        return future

    def clear(self):
        pass

    def settings_module(self):
        future = Future()
        future.set_result('test_project.settings')
        return future


class TestRunnerFactoryStub:
    _runner_cls = None

    def __init__(self, runner_cls=None):
        self._runner_cls = runner_cls

    def build(self, eggf):
        if self._runner_cls:
            return self._runner_cls(eggf)
        return TestRunnerStub(eggf)


class AppTest(AsyncHTTPTestCase):
    @classmethod
    def setUpClass(cls):
        if os._exists('test.db'):
            os.remove('test.db')
        config = Config(values = {'database_url': 'sqlite:///test.db'})
        init_database(config)
        os.environ['ASYNC_TEST_TIMEOUT'] = '120'
        AppTest.init_project()

    @classmethod
    def get_user(self):
        with session_scope() as session:
            return session.query(User).get(1)

    @classmethod
    def init_project(self):
        project_name = 'test_project'
        version = '1.0'
        spiders = ['error_spider',
                    'fail_spider',
                    'log_spider',
                    'success_spider',
                    'warning_spider']

        ioloop = IOLoop.current()
        with open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb') as egg_file:
            def fun():
                return AppTest.project_manager.upload_project(self.get_user(), project_name, version, egg_file)
            project = ioloop.run_sync(fun)
            self.project = project
            return project


    runner_factory = TestRunnerFactoryStub()
    project_storage_dir = './test_data'
    os.environ['SCRAPYDD_PROJECT_STORAGE_DIR'] = './test_data'
    default_project_storage_version = 2
    project_manager = ProjectManager(runner_factory, project_storage_dir, default_project_storage_version)
    scheduler_manager = None

    def get_app(self):
        config = Config()
        if self.scheduler_manager is None:
            self.scheduler_manager = SchedulerManager(config)
        self.scheduler_manager.init()
        node_manager = NodeManager(self.scheduler_manager)
        node_manager.init()
        webhook_daemon = WebhookDaemon(config, SpiderSettingLoader())
        webhook_daemon.init()
        return make_app(self.scheduler_manager, node_manager, webhook_daemon, secret_key='123',
                        project_storage_dir=self.project_storage_dir,
                        runner_factory=self.runner_factory,
                        project_manager=self.project_manager)

    def _upload_test_project(self):
        post_data = {}
        post_data['egg'] = open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = 'test_project'
        post_data['version'] = '1.0'

        datagen, headers = multipart_encode(post_data)
        databuffer = b''.join(datagen)
        response = self.fetch('/addversion.json', method='POST', headers=headers, body=databuffer)
        self.assertEqual(200, response.code)
        post_data['egg'].close()

class SecureAppTest(AppTest):
    secret_key = '123'

    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        secret_key = '123'
        with session_scope() as session:
            user = session.query(User).filter_by(username='admin').first()
            user.password = encrypt_password('password', secret_key)
            session.add(user)
            session.commit()

            normal_user = session.query(User).filter_by(username='adam').first()
            if not normal_user:
                normal_user = User()
                normal_user.username = 'adam'
            normal_user.is_admin = False
            normal_user.password = encrypt_password('passw0rd', secret_key)
            session.add(normal_user)
            session.commit()

        return make_app(scheduler_manager, node_manager, None, secret_key='123', enable_authentication=True)

    def populate_basic_authorization_header(self, headers):
        headers = HTTPHeaders(headers)
        headers['Authorization'] = basic_auth_header('admin', 'password')
        return headers

    def populate_cookie_header(self, headers, username='admin'):
        cookie_name, cookie_value = 'user', username
        secure_cookie = create_signed_value(
            self.get_app().settings["cookie_secret"],
            cookie_name,
            cookie_value)
        headers = HTTPHeaders(headers)
        headers.add('Cookie', b'='.join((ensure_binary(cookie_name), ensure_binary(secure_cookie))))
        return headers

    def populate_xsrf_cookie(self, headers):
        headers = HTTPHeaders(headers)
        headers.add("Cookie", "_xsrf=dummy")
        headers.add('X-XSRFToken', 'dummy')
        return headers

    def _upload_test_project(self):
        post_data = {}
        post_data['egg'] = open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = 'test_project'
        post_data['version'] = '1.0'

        datagen, headers = multipart_encode(post_data)
        self.populate_basic_authorization_header(headers)
        databuffer = ''.join(datagen)
        response = self.fetch('/addversion.json', method='POST', headers=headers, body=databuffer)
        self.assertEqual(200, response.code)
