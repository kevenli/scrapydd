from unittest import TestCase
from scrapydd.main import *
from scrapydd.config import Config
from scrapydd.models import init_database
from tornado.testing import AsyncHTTPTestCase, gen_test
from tornado.web import create_signed_value
from scrapydd.poster.encode import multipart_encode
import os.path
import logging
from .base import AppTest
from six import ensure_binary, ensure_str
from six.moves.urllib.parse import urlencode
from scrapydd.stream import MultipartRequestBodyProducer

logger = logging.getLogger(__name__)


class MainTest(AsyncHTTPTestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['ASYNC_TEST_TIMEOUT'] = '500'
        if os._exists('test.db'):
            os.remove('test.db')
        config = Config(values = {'database_url': 'sqlite:///test.db'})
        init_database(config)

    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        runner_factory = RunnerFactory(config)
        return make_app(scheduler_manager, node_manager, None, secret_key='123', runner_factory=runner_factory)

    def _delproject(self):
        postdata = {'project': 'test_project'}
        response = self.fetch('/delproject.json', method='POST', body=urlencode(postdata))
        self.assertIn(response.code, [404, 200])

    def _upload_test_project(self):
        # upload a project

        post_data = {}
        post_data['egg'] = open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = 'test_project'
        post_data['version'] = '1.0'

        datagen, headers = multipart_encode(post_data)
        databuffer = ''.join(datagen)
        response = self.fetch('/addversion.json', method='POST', headers=headers, body=databuffer)
        self.assertEqual(200, response.code)


class DefaultTest(MainTest):
    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        return make_app(scheduler_manager, node_manager, None, secret_key='123')

    def test_default_page(self):
        response = self.fetch('/')
        self.assertEqual(200, response.code)


class SecurityTest(MainTest):
    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        return make_app(scheduler_manager, node_manager, None, enable_authentication=True, secret_key='123')

    def test_no_cookie(self):
        response = self.fetch('/',follow_redirects=False)
        self.assertEqual(302, response.code)

    def test_with_cookie(self):
        username = 'test'
        cookie_name, cookie_value = 'user', username
        secure_cookie = ensure_str(create_signed_value(
            self.get_app().settings["cookie_secret"],
            cookie_name,
            cookie_value))
        headers = {'Cookie': '='.join((cookie_name, secure_cookie))}
        response = self.fetch('/', method='GET', headers=headers)
        self.assertEqual(200, response.code)


class UploadTest(MainTest):
    def test_logging_init(self):
        self.skipTest('no logging init')

    def test_MainHandler(self):
        response = self.fetch('/')
        self.assertEqual(200, response.code)


class UploadTest2(AppTest):
    def test_logging_init(self):
        self.skipTest('no logging init')

    def test_MainHandler(self):
        response = self.fetch('/')
        self.assertEqual(200, response.code)

    def test_UploadProject_post(self):
        project_name = 'test_project'
        post_data = {}
        post_data['egg'] = open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = project_name
        post_data['version'] = '1.0'
        post_data['_xsrf'] = 'dummy'

        datagen, headers = multipart_encode(post_data)
        databuffer = b''.join([ensure_binary(x) for x in datagen])
        headers['Cookie'] = "_xsrf=dummy"
        response = self.fetch('/uploadproject', method='POST', headers=headers, body=databuffer)

        self.assertEqual(200, response.code)

        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()
            self.assertIsNotNone(project)
            self.assertEqual(project.name, project_name)


class ScheduleHandlerTest(AppTest):
    def test_post(self):
        from scrapydd.models import SpiderExecutionQueue
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            session.commit()
        # schedule once
        project = 'test_project'
        spider = 'success_spider'
        postdata = urlencode({
            'project': project,
            'spider': spider
        })
        response = self.fetch('/schedule.json', method='POST', body=postdata)
        self.assertEqual(200, response.code)

    def test_post_job_already_running(self):
        project = 'test_project'
        spider = 'success_spider'
        postdata = urlencode({
            'project': project,
            'spider': spider
        })
        self.fetch('/schedule.json', method='POST', body=postdata)
        response = self.fetch('/schedule.json', method='POST', body=postdata)
        self.assertEqual(400, response.code)
        self.assertIn(b'job is running', response.body)


class AddScheduleHandlerTest(AppTest):
    def test_add_scheduler(self):
        project = 'test_project'
        spider = 'success_spider'
        cron = '* * * * *'

        postdata = {
            'project':project,
            'spider':spider,
            'cron':cron,
            '_xsrf':'dummy',
        }

        response = self.fetch('/add_schedule.json', method='POST', body=urlencode(postdata),
                              headers={"Cookie": "_xsrf=dummy"})
        self.assertEqual(200, response.code)
        self.assertIn(b'ok', response.body)


class ProjectListTest(MainTest):
    def test_get(self):
        response = self.fetch('/projects')
        self.assertEqual(200, response.code)


class SpiderInstanceHandlerTest(MainTest):
    def test_get(self):
        spider = 'success_spider'
        response = self.fetch('/')


class NodesHandlerTest(MainTest):
    def test_register(self):
        with session_scope() as session:
            session.query(Node).delete()

        response = self.fetch('/nodes', method="POST", body="")


        with session_scope() as session:
            new_node = session.query(Node).first()

        self.assertEqual(200, response.code)
        self.assertEqual('127.0.0.1', new_node.client_ip)
        self.assertEqual(datetime.date.today(), new_node.create_time.date())
        self.assertEqual(datetime.date.today(), new_node.last_heartbeat.date())
        self.assertEqual(True, new_node.isalive)
        self.assertEqual(None, new_node.tags)


class SpiderInstanceHandler2Test(AppTest):
    def test_get(self):
        with session_scope() as session:
            spider = session.query(Spider).first()
            project = spider.project

        self.assertIsNotNone(spider)
        response = self.fetch('/projects/%s/spiders/%s' % (project.name, spider.name))
        self.assertEqual(200, response.code)


class SpiderEggHandlerTest(AppTest):
    def test_get(self):
        self._upload_test_project()
        with session_scope() as session:
            spider = session.query(Spider).first()
            project = spider.project

        self.assertIsNotNone(spider)
        response = self.fetch('/spiders/%d/egg' % (spider.id, ))
        self.assertEqual(200, response.code)

    def test_get_egg_by_project_spider_name(self):
        self._upload_test_project()
        with session_scope() as session:
            spider = session.query(Spider).first()
            project = spider.project

        self.assertIsNotNone(spider)
        response = self.fetch('/projects/%s/spiders/%s/egg' % ('test_project', 'log_spider'))
        self.assertEqual(200, response.code)