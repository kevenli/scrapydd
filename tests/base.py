from tornado.testing import AsyncHTTPTestCase
from poster.encode import multipart_encode
from scrapydd.security import encrypt_password
from w3lib.http import basic_auth_header
from scrapydd.models import init_database, session_scope, User, Project, Spider
from scrapydd.config import Config
import os
from scrapydd.schedule import SchedulerManager
from scrapydd.nodes import NodeManager
from scrapydd.main import make_app
from scrapydd.eggstorage import FilesystemEggStorage
from tornado.web import create_signed_value
from tornado.httputil import HTTPHeaders


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
    def init_project(self):
        project_name = 'test_project'
        version = '1.0'
        spiders = ['error_spider',
                    'fail_spider',
                    'log_spider',
                    'success_spider',
                    'warning_spider']
        egg_file = open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb')
        with session_scope() as session:
            storage = FilesystemEggStorage({})
            egg_file.seek(0)
            storage.put(egg_file, project_name, version)
            project = session.query(Project).filter_by(name=project_name).first()
            if project is None:
                project = Project()
                project.name = project_name
            project.version = version
            session.add(project)
            session.commit()
            session.refresh(project)

            for spider_name in spiders:
                spider = session.query(Spider).filter_by(project_id=project.id, name=spider_name).first()
                if spider is None:
                    spider = Spider()
                    spider.name = spider_name
                    spider.project_id = project.id
                    session.add(spider)
                    session.commit()
                    session.refresh(spider)

                session.commit()

    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        return make_app(scheduler_manager, node_manager, None, secret_key='123')

    def _upload_test_project(self):
        post_data = {}
        post_data['egg'] = open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = 'test_project'
        post_data['version'] = '1.0'

        datagen, headers = multipart_encode(post_data)
        databuffer = ''.join(datagen)
        response = self.fetch('/addversion.json', method='POST', headers=headers, body=databuffer)
        self.assertEqual(200, response.code)

class SecureAppTest(AppTest):
    secret_key = '123'

    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        secret_key = '123';
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
        headers.add('Cookie', '='.join((cookie_name, secure_cookie)))
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
