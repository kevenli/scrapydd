from os import path
from six.moves.urllib.parse import urlencode
from tornado.concurrent import Future
from unittest import skip
from scrapydd.poster.encode import multipart_encode, ensure_binary
from scrapydd.models import Project, session_scope
from scrapydd.main import make_app
from scrapydd.schedule import SchedulerManager
from scrapydd.nodes import NodeManager
from scrapydd.webhook import WebhookDaemon
from scrapydd.settting import SpiderSettingLoader
from scrapydd.config import Config
from scrapydd.exceptions import InvalidProjectEgg, ProcessFailed
from ..base import AppTest, ProjectWorkspaceStub


base_dir = path.join(path.dirname(path.dirname(__file__)))


class AddVersionHandlerTest(AppTest):
    def test_post(self):
        project_name = 'test_project'
        post_data = {}
        post_data['egg'] = open(path.join(base_dir, 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = project_name
        post_data['version'] = '1.0'
        post_data['_xsrf'] = 'dummy'

        datagen, headers = multipart_encode(post_data)
        databuffer = b''.join([ensure_binary(x) for x in datagen])
        headers['Cookie'] = "_xsrf=dummy"
        response = self.fetch('/addversion.json', method='POST', headers=headers, body=databuffer)

        self.assertEqual(200, response.code)

        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()
            self.assertIsNotNone(project)
            self.assertEqual(project.name, project_name)

    def test_post_create(self):
        project_name = 'test_project'
        postdata = {'project': project_name}
        response = self.fetch('/delproject.json', method='POST', body=urlencode(postdata))
        self.assertIn(response.code, [404, 200])
        post_data = {}
        post_data['egg'] = open(path.join(base_dir, 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = project_name
        post_data['version'] = '1.0'
        post_data['_xsrf'] = 'dummy'

        datagen, headers = multipart_encode(post_data)
        databuffer = b''.join([ensure_binary(x) for x in datagen])
        headers['Cookie'] = "_xsrf=dummy"
        response = self.fetch('/addversion.json', method='POST', headers=headers, body=databuffer)

        self.assertEqual(200, response.code)

        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()
            self.assertIsNotNone(project)
            self.assertEqual(project.name, project_name)

class AddVersionHandlerTest_InvalidProjectEgg(AppTest):
    class InvalidProjectWorkspaceStub(ProjectWorkspaceStub):
        def spider_list(self):
            future = Future()
            future.set_exception(InvalidProjectEgg())
            return future

    @skip
    def test_post(self):
        project_name = 'test_project'
        post_data = {}
        post_data['egg'] = open(path.join(base_dir, 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = project_name
        post_data['version'] = '1.0'
        post_data['_xsrf'] = 'dummy'

        datagen, headers = multipart_encode(post_data)
        databuffer = b''.join([ensure_binary(x) for x in datagen])
        headers['Cookie'] = "_xsrf=dummy"
        response = self.fetch('/addversion.json', method='POST', headers=headers, body=databuffer)

        self.assertEqual(400, response.code)

    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        webhook_daemon = WebhookDaemon(config, SpiderSettingLoader())
        webhook_daemon.init()
        return make_app(scheduler_manager, node_manager, webhook_daemon, secret_key='123',
                        project_workspace_cls=AddVersionHandlerTest_InvalidProjectEgg.InvalidProjectWorkspaceStub,
                        project_storage_dir='./test_data')


class AddVersionHandlerTest_ProcessFail(AppTest):
    class ProcessFailProjectWorkspaceStub(ProjectWorkspaceStub):
        def spider_list(self):
            future = Future()
            future.set_exception(ProcessFailed())
            return future

    @skip
    def test_post(self):
        project_name = 'test_project'
        post_data = {}
        post_data['egg'] = open(path.join(base_dir, 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = project_name
        post_data['version'] = '1.0'
        post_data['_xsrf'] = 'dummy'

        datagen, headers = multipart_encode(post_data)
        databuffer = b''.join([ensure_binary(x) for x in datagen])
        headers['Cookie'] = "_xsrf=dummy"
        response = self.fetch('/addversion.json', method='POST', headers=headers, body=databuffer)

        self.assertEqual(400, response.code)

    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        webhook_daemon = WebhookDaemon(config, SpiderSettingLoader())
        webhook_daemon.init()
        return make_app(scheduler_manager, node_manager, webhook_daemon, secret_key='123',
                        project_workspace_cls=AddVersionHandlerTest_ProcessFail.ProcessFailProjectWorkspaceStub,
                        project_storage_dir='./test_data')


class DeleteProjectHandlerTest(AppTest):
    def test_post(self):
        project_name = 'test_project'
        postdata = {'project': project_name}
        response = self.fetch('/delproject.json', method='POST', body=urlencode(postdata))
        self.assertIn(response.code, [404, 200])

        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()
            self.assertIsNone(project)