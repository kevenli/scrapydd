"""
Tests for rest api handlers
"""
# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
from os import path
from six.moves.urllib.parse import urlencode
from tornado.concurrent import Future
from scrapydd.poster.encode import multipart_encode, ensure_binary
from scrapydd.models import Project, session_scope
from scrapydd.main import make_app
from scrapydd.schedule import SchedulerManager
from scrapydd.nodes import NodeManager
from scrapydd.webhook import WebhookDaemon
from scrapydd.settting import SpiderSettingLoader
from scrapydd.config import Config
from scrapydd.exceptions import InvalidProjectEgg, ProcessFailed
from tests.base import AppTest, TestRunnerStub, TestRunnerFactoryStub


BASE_DIR = path.join(path.dirname(path.dirname(__file__)))
TEST_EGG_FILE = path.join(BASE_DIR, 'test_project-1.0-py2.7.egg')


class AddVersionHandlerTest(AppTest):
    def test_post(self):
        project_name = 'test_project'
        post_data = {}
        post_data['egg'] = open(TEST_EGG_FILE, 'rb')
        post_data['project'] = project_name
        post_data['version'] = '1.0'
        post_data['_xsrf'] = 'dummy'

        datagen, headers = multipart_encode(post_data)
        databuffer = b''.join([ensure_binary(x) for x in datagen])
        headers['Cookie'] = "_xsrf=dummy"
        response = self.fetch('/addversion.json',
                              method='POST', headers=headers, body=databuffer)

        self.assertEqual(200, response.code)

        with session_scope() as session:
            project = session.query(Project)\
                .filter_by(name=project_name).first()
            self.assertIsNotNone(project)
            self.assertEqual(project.name, project_name)

    def test_post_create(self):
        project_name = 'test_project'
        postdata = {'project': project_name}
        response = self.fetch('/delproject.json',
                              method='POST', body=urlencode(postdata))
        self.assertIn(response.code, [404, 200])
        post_data = {}
        post_data['egg'] = open(TEST_EGG_FILE, 'rb')
        post_data['project'] = project_name
        post_data['version'] = '1.0'
        post_data['_xsrf'] = 'dummy'

        datagen, headers = multipart_encode(post_data)
        databuffer = b''.join([ensure_binary(x) for x in datagen])
        headers['Cookie'] = "_xsrf=dummy"
        response = self.fetch('/addversion.json', method='POST',
                              headers=headers, body=databuffer)

        self.assertEqual(200, response.code)

        with session_scope() as session:
            project = session.query(Project)\
                .filter_by(name=project_name).first()
            self.assertIsNotNone(project)
            self.assertEqual(project.name, project_name)


class AddVersionHandlerTestInvalidProjectEgg(AppTest):
    class InvalidProjectWorkspaceStub(TestRunnerStub):
        def list(self):
            future = Future()
            future.set_exception(InvalidProjectEgg())
            return future

    def test_post(self):
        project_name = 'test_project'
        post_data = {}
        post_data['egg'] = open(TEST_EGG_FILE, 'rb')
        post_data['project'] = project_name
        post_data['version'] = '1.0'
        post_data['_xsrf'] = 'dummy'

        datagen, headers = multipart_encode(post_data)
        databuffer = b''.join([ensure_binary(x) for x in datagen])
        headers['Cookie'] = "_xsrf=dummy"
        response = self.fetch('/addversion.json', method='POST',
                              headers=headers, body=databuffer)

        self.assertEqual(400, response.code)

    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        webhook_daemon = WebhookDaemon(config, SpiderSettingLoader())
        webhook_daemon.init()
        runner_cls = AddVersionHandlerTestInvalidProjectEgg.\
            InvalidProjectWorkspaceStub
        runner_factory = TestRunnerFactoryStub(runner_cls)
        return make_app(scheduler_manager, node_manager,
                        webhook_daemon, secret_key='123',
                        project_storage_dir='./test_data',
                        runner_factory=runner_factory)


class AddVersionHandlerTestProcessFail(AppTest):
    class ProcessFailProjectWorkspaceStub(TestRunnerStub):
        def list(self):
            future = Future()
            future.set_exception(ProcessFailed())
            return future

    def test_post(self):
        project_name = 'test_project'
        post_data = {}
        post_data['egg'] = open(TEST_EGG_FILE, 'rb')
        post_data['project'] = project_name
        post_data['version'] = '1.0'
        post_data['_xsrf'] = 'dummy'

        datagen, headers = multipart_encode(post_data)
        databuffer = b''.join([ensure_binary(x) for x in datagen])
        headers['Cookie'] = "_xsrf=dummy"
        response = self.fetch('/addversion.json', method='POST',
                              headers=headers, body=databuffer)

        self.assertEqual(400, response.code)

    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        webhook_daemon = WebhookDaemon(config, SpiderSettingLoader())
        webhook_daemon.init()
        runner_cls = AddVersionHandlerTestProcessFail.ProcessFailProjectWorkspaceStub
        runner_factory = TestRunnerFactoryStub(runner_cls)
        return make_app(scheduler_manager, node_manager, webhook_daemon,
                        secret_key='123',
                        project_storage_dir='./test_data',
                        runner_factory=runner_factory)


class DeleteProjectHandlerTest(AppTest):
    def test_post(self):
        # TODO: create a project and then delete it.
        project_name = 'DeleteProjectHandlerTest'
        postdata = {'project': project_name}
        response = self.fetch('/delproject.json', method='POST',
                              body=urlencode(postdata))
        self.assertIn(response.code, [404, 200])

        with session_scope() as session:
            project = session.query(Project)\
                .filter_by(name=project_name).first()
            self.assertIsNone(project)


class AddScheduleHandlerTest(AppTest):
    def test_post(self):
        project_name = 'test_project'
        spider = 'success_spider'
        cron = '* * * * *'
        post_data = {
            'project': project_name,
            'spider': spider,
            'cron': cron,
            '_xsrf': 'dummy',
        }

        response = self.fetch('/add_schedule.json', method='POST',
                              body=urlencode(post_data),
                              headers={"Cookie": "_xsrf=dummy"})
        self.assertEqual(200, response.code)
        self.assertIn(b'ok', response.body)

    def test_project_not_found(self):
        project_name = 'test_project_NOT_EXIST'
        spider = 'success_spider'
        cron = '* * * * *'
        post_data = {
            'project': project_name,
            'spider': spider,
            'cron': cron,
            '_xsrf': 'dummy',
        }

        response = self.fetch('/add_schedule.json', method='POST',
                              body=urlencode(post_data),
                              headers={"Cookie": "_xsrf=dummy"})
        self.assertEqual(404, response.code)

    def test_spider_not_found(self):
        project_name = 'test_project'
        spider = 'success_spider_NOT_EXIST'
        cron = '* * * * *'
        post_data = {
            'project': project_name,
            'spider': spider,
            'cron': cron,
            '_xsrf': 'dummy',
        }

        response = self.fetch('/add_schedule.json', method='POST',
                              body=urlencode(post_data),
                              headers={"Cookie": "_xsrf=dummy"})
        self.assertEqual(404, response.code)

    def test_spider_invalid_cron(self):
        project_name = 'test_project'
        spider = 'success_spider'
        cron = '* * * * * BLABLABLA'
        post_data = {
            'project': project_name,
            'spider': spider,
            'cron': cron,
            '_xsrf': 'dummy',
        }

        response = self.fetch('/add_schedule.json', method='POST',
                              body=urlencode(post_data),
                              headers={"Cookie": "_xsrf=dummy"})
        self.assertEqual(400, response.code)
