from os import path
from io import BytesIO
from six.moves.urllib.parse import urlencode
from scrapydd.storage import ProjectStorage
from scrapydd.models import session_scope, Project, Spider
from scrapydd.poster.encode import multipart_encode
from scrapydd.schedule import JOB_STATUS_SUCCESS
from ..base import AppTest


class TestDeleteProjectHandler(AppTest):
    def test_post(self):
        project_name = 'test_project'
        self._upload_test_project()
        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()

            project_storage = ProjectStorage(self._app.settings.get('project_storage_dir'), project)

            self.assertTrue(path.exists(project_storage.storage_provider.get_project_eggs_dir(project)))

            headers = {'Cookie': "_xsrf=dummy"}
            post_data = {'_xsrf': 'dummy'}
            res = self.fetch('/projects/%s/delete' % project.id, method="POST", headers=headers,
                             body=urlencode(post_data))
            self.assertEqual(200, res.code)

            # do not delete folder
            # self.assertFalse(path.exists(project_storage.storage_provider.get_project_eggs_dir(project)))
            self.assertEqual(len(project_storage.list_egg_versions()), 0)

            self.assertIsNone(session.query(Project).filter_by(name=project_name).first())

            self.assertEqual(0, len(session.query(Spider).filter_by(project_id=project.id).all()))

    def test_post_with_triggers(self):
        project_name = 'test_project'
        spider_name = 'error_spider'
        self._upload_test_project()

        headers = {'Cookie': "_xsrf=dummy"}
        with session_scope() as session:
            project = session.query(Project)\
                .filter_by(name=project_name)\
                .first()
            spider = list(filter(lambda x: x.name==spider_name, project.spiders))[0]

            post_data = {'_xsrf': 'dummy', 'cron': '0 0 0 0 0'}
            res = self.fetch('/projects/%s/spiders/%s/triggers' % (project.id,
                                                             spider.id),
                             method='POST',
                             headers=headers,
                             body=urlencode(post_data))
            self.assertEqual(200, res.code)


            post_data = {'_xsrf': 'dummy'}
            res = self.fetch('/projects/%s/delete' % project.id,
                             method="POST",
                             headers=headers,
                             body=urlencode(post_data))
            self.assertEqual(200, res.code)


class RunSpiderHandlerTest(AppTest):
    def init_project(self, project_name):
        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()
            if project:
                self.project_manager.delete_project('', project.id)
        self.project = AppTest.init_project()

    def test_post_insecure(self):
        project_name = 'test_project'
        spider_name = 'error_spider'
        url = '/projects/%s/spiders/%s/run' % (project_name, spider_name)
        res = self.fetch(url, method='POST', body=b'')
        self.assertEqual(403, res.code)

    def test_post(self):
        project_name = 'test_project'
        spider_name = 'error_spider'
        self.init_project(project_name)
        with session_scope() as session:
            project = session.query(Project).get(self.project.id)
            spider = list(filter(lambda x: x.name == spider_name, project.spiders))[0]
        url = '/projects/%s/spiders/%s/run' % (self.project.id, spider.id)
        headers = {'Cookie': "_xsrf=dummy"}
        post_data = {'_xsrf': 'dummy'}
        res = self.fetch(url, method='POST', headers=headers, body=urlencode(post_data))
        self.assertEqual(200, res.code)

    def test_post_no_project(self):
        project_name = 'PROJECT_NOT_EXIST'
        spider_name = 'error_spider'
        self.init_project(project_name)

        url = '/projects/%s/spiders/%s/run' % (project_name, spider_name)
        headers = {'Cookie': "_xsrf=dummy"}
        post_data = {'_xsrf': 'dummy'}
        res = self.fetch(url, method='POST', headers=headers, body=urlencode(post_data))
        self.assertEqual(404, res.code)

    def test_post_no_spider(self):
        project_name = 'test_project'
        spider_name = 'SPIDER_NOT_EXIST'
        self.init_project(project_name)
        url = '/projects/%s/spiders/%s/run' % (project_name, spider_name)
        headers = {'Cookie': "_xsrf=dummy"}
        post_data = {'_xsrf': 'dummy'}
        res = self.fetch(url, method='POST', headers=headers, body=urlencode(post_data))
        self.assertEqual(404, res.code)


class ProjectSettingsHandlerTest(AppTest):
    def test_get(self):
        project_name = 'test_project'
        self.init_project()
        url = '/projects/%s/settings' % (self.project.id, )
        res = self.fetch(url, method='GET')
        self.assertEqual(200, res.code)


class UploadProjectTest(AppTest):
    def test_get(self):
        url = '/uploadproject'
        res = self.fetch(url, method='GET')
        self.assertEqual(200, res.code)

    def test_post(self):
        project_name = 'test_project'
        post_data = {}
        post_data['egg'] = open(path.join(path.dirname(__file__), '..', 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = project_name
        post_data['version'] = '1.0'
        post_data['_xsrf'] = 'dummy'

        datagen, headers = multipart_encode(post_data)
        databuffer = b''.join(datagen)
        headers['Cookie'] = "_xsrf=dummy"
        response = self.fetch('/uploadproject', method='POST', headers=headers, body=databuffer)

        self.assertEqual(200, response.code)

        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()
            self.assertIsNotNone(project)
            self.assertEqual(project.name, project_name)


class ItemsFileHandlerTest(AppTest):
    def test_get(self):
        project = self.init_project()
        with session_scope() as session:
            project = session.query(Project).get(self.project.id)
            spider = list(filter(lambda x: x.name == 'error_spider', project.spiders))[0]
            project_id = None
            spider_id = None
            job_id = None

            job = self.scheduler_manager.add_spider_task(session, spider)
        items_content = b'{"id":"123"}'
        items_file = BytesIO(items_content)
        self.scheduler_manager.jobs_running(1, [job.id])

        job.status = JOB_STATUS_SUCCESS
        self.scheduler_manager.job_finished(job, items_file=items_file)

        response = self.fetch('/items/%s/%s/%s.jl' % (project.id, spider.id, job.id))
        self.assertEqual(200, response.code)

