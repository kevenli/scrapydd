import os
from unittest import TestCase
import logging
from tornado.testing import AsyncTestCase, gen_test
from scrapydd.workspace import RunnerFactory
from scrapydd.project import ProjectManager
from scrapydd.schedule import SchedulerManager
from scrapydd.config import Config
from scrapydd.models import init_database, session_scope, Session, User
from scrapydd.exceptions import ProjectNotFound


logger = logging.getLogger(__name__)


class ProjectManagerTest(TestCase):
    def setUp(self) -> None:
        if os.path.exists('test.db'):
            os.remove('test.db')
        self.config = Config(values={'database_url': 'sqlite:///test.db'})
        init_database()

        self.session = Session()
        self.user = self.session.query(User).get(1)

        self.runner_factory = RunnerFactory(self.config)
        self.project_storage_dir = 'test_data'
        self.scheduler = SchedulerManager()
        self.default_project_storage_version = 2

    def buildProjectManager(self):
        return ProjectManager(self.runner_factory,
                              self.project_storage_dir,
                              self.scheduler,
                              self.default_project_storage_version)


class ProjectCreateTest(ProjectManagerTest):
    def test_create_project(self):
        target = self.buildProjectManager()
        session = self.session
        user = self.user
        project_name = 'test_create_project'
        projects = target.get_projects(session, user)
        try:
            existing_project = next(filter(lambda x: x.name == project_name, projects))
            target.delete_project(user.id, existing_project.id)
        except StopIteration:
            pass

        project = target.create_project(session, user, project_name)

        self.assertEqual(project.name, project_name)

    def test_delet_project(self):
        target = self.buildProjectManager()
        session = self.session
        user = self.user
        project_name = 'test_create_project'
        projects = target.get_projects(session, user)
        try:
            existing_project = next(filter(lambda x: x.name == project_name, projects))
            target.delete_project(user.id, existing_project.id)
        except StopIteration:
            pass

        project = target.create_project(session, user, project_name)
        project_id = project.id
        target.delete_project(user.id, project.id)
        try:
            project_after_delete = target.get_project(session, user, project_id)
            self.fail("Can still get project instance")
        except ProjectNotFound:
            pass


class ProjectUploadPackageTest(AsyncTestCase, ProjectManagerTest):
    @gen_test(timeout=300)
    def test_upload(self):
        target = self.buildProjectManager()
        session = self.session
        user = self.user
        project_name = 'test_upload_project'
        projects = target.get_projects(session, user)
        version = '1.1'
        try:
            existing_project = next(filter(lambda x: x.name == project_name, projects))
            target.delete_project(user.id, existing_project.id)
        except StopIteration:
            pass

        project = target.create_project(session, user, project_name)

        test_project_file = os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg')
        with open(test_project_file, 'rb') as f_egg:
            project = yield target.upload_project_package(session, project,
                                                          f_egg, version)
        self.assertIsNotNone(project)
        self.assertEqual(project.package.spider_list, ','.join(['error_spider', 'fail_spider',
                                                                 'log_spider', 'sina_news', 'success_spider',
                          'warning_spider']))

        self.assertEqual(len(project.packages), 1)
        self.assertEqual(project.packages[0].project, project)
        self.assertEqual(project.packages[0].type, 'scrapy')
        self.assertEqual(project.packages[0].version, 1)
        self.assertEqual(project.packages[0].egg_version, '1.2')
        #self.assertEqual(project.packages[0].checksum, checksum)
        self.assertEqual(project.packages[0].spider_list, ','.join(['error_spider', 'fail_spider',
                                                                 'log_spider', 'sina_news', 'success_spider',
                          'warning_spider']))
        self.assertIsNotNone(project.packages[0].file_path)
        self.assertTrue(os.path.exists(project.packages[0].file_path))
