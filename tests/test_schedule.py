import unittest
from scrapydd.schedule import SchedulerManager
import logging
from scrapydd.models import Session, HistoricalJob, init_database
from tornado.testing import AsyncHTTPTestCase
import os
from scrapydd.config import Config
from scrapydd.nodes import NodeManager
from scrapydd.main import *
from scrapydd.poster.encode import multipart_encode
from scrapydd.stream import MultipartRequestBodyProducer
import urllib
from scrapydd.schedule import *
from .base import AppTest

class ScheduleTest(AsyncHTTPTestCase):
    @classmethod
    def setUpClass(cls):
        if os._exists('test.db'):
            os.remove('test.db')
        config = Config(values={'database_url': 'sqlite:///test.db'})
        init_database(config)
        os.environ['ASYNC_TEST_TIMEOUT'] = '30'

    def setUp(self):
        super(ScheduleTest, self).setUp()
        self._upload_test_project()


    def _delproject(self):
        postdata = {'project': 'test_project'}
        self.fetch('/delproject.json', method='POST', body=urllib.urlencode(postdata))


    def _set_spider_tag(self, project_name, spider_name, tag):
        with session_scope() as session:
            project = session.query(Project).filter(Project.name == project_name).first()
            spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider_name).first()
            setting_tag = session.query(SpiderSettings).filter_by(spider_id=spider.id,
                                                                  setting_key='tag').first()
            if not setting_tag:
                setting_tag = SpiderSettings()
                setting_tag.spider_id = spider.id
                setting_tag.setting_key = 'tag'
            setting_tag.value = tag
            session.add(setting_tag)

    def _set_spider_setting(self, project_name, spider_name, setting_key, setting_value):
        with session_scope() as session:
            project = session.query(Project).filter(Project.name == project_name).first()
            spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider_name).first()
            setting = session.query(SpiderSettings).filter_by(spider_id=spider.id,
                                                                  setting_key=setting_key).first()
            if not setting:
                setting = SpiderSettings()
                setting.spider_id = spider.id
                setting.setting_key = setting_key
            setting.value = setting_value
            session.add(setting)

    def get_app(self):
        config = Config()
        self.scheduler_manager = scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        self.node_manager = node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        return make_app(scheduler_manager, node_manager, None)

    def _upload_test_project(self):
        # upload a project

        post_data = {}
        post_data['egg'] = open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = 'test_project'
        post_data['version'] = '1.0'

        datagen, headers = multipart_encode(post_data)
        self.fetch('/addversion.json', method='POST', headers=headers,
                   body_producer=MultipartRequestBodyProducer(datagen))


class SchedulerManagerTest(unittest.TestCase):
    def setUp(self):
        if os._exists('test.db'):
            os.remove('test.db')
        config = Config(values = {'database_url': 'sqlite:///test.db'})
        init_database(config)

    @unittest.skip
    def test_clear_finised_jobs(self):
        target = SchedulerManager()
        target._clear_running_jobs()
        jobids = []
        spider_id = 1
        for i in range(0, 101):
            job = target.add_task('weibo', 'discover')
            jobids.append(job.id)
            target.job_start(job.id, 0)
            job.status = 2
            target.job_finished(job)

        jobids = jobids[-100:]
        target.clear_finished_jobs()

        session = Session()
        for job in session.query(HistoricalJob).filter(HistoricalJob.spider_id==spider_id):
            self.assertTrue(job.id in jobids)
        session.close()


class ScheduleTagTest(AppTest):
    # @classmethod
    # def setUpClass(cls):
    #     if os._exists('test.db'):
    #         os.remove('test.db')
    #     config = Config(values = {'database_url': 'sqlite:///test.db'})
    #     init_database(config)
    #     os.environ['ASYNC_TEST_TIMEOUT'] = '30'
    #
    def setUp(self):
        super(ScheduleTagTest, self).setUp()
        # self._delproject()
        # self._upload_test_project()

        with session_scope() as session:
           session.query(SpiderExecutionQueue).delete()
    #
    # def _delproject(self):
    #     postdata = {'project': 'test_project'}
    #     self.fetch('/delproject.json', method='POST', body=urllib.urlencode(postdata))
    #
    #
    def _set_spider_tag(self, project_name, spider_name, tag):
        with session_scope() as session:
            project = session.query(Project).filter(Project.name == project_name).first()
            spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider_name).first()
            setting_tag = session.query(SpiderSettings).filter_by(spider_id=spider.id,
                                                                  setting_key='tag').first()
            if not setting_tag:
                setting_tag = SpiderSettings()
                setting_tag.spider_id = spider.id
                setting_tag.setting_key = 'tag'
            setting_tag.value = tag
            session.add(setting_tag)
            pass
    #
    def get_app(self):
        config = Config()
        self.scheduler_manager = scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        self.node_manager = node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        return make_app(scheduler_manager, node_manager, None)
    #
    # def _upload_test_project(self):
    #     # upload a project
    #
    #     post_data = {}
    #     post_data['egg'] = open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb')
    #     post_data['project'] = 'test_project'
    #     post_data['version'] = '1.0'
    #
    #     datagen, headers = multipart_encode(post_data)
    #     self.fetch('/addversion.json', method='POST', headers=headers,
    #                body_producer=MultipartRequestBodyProducer(datagen))

    # agent tags: None, spider tag: None
    def test_none_none(self):
        agent_tags = None
        spider_tag = None
        node = self.node_manager.create_node('*', tags = agent_tags)
        project_name = 'test_project'
        spider_name = 'success_spider'

        self._set_spider_tag(project_name, spider_name, spider_tag)

        target = SchedulerManager()
        job = target.add_task('test_project', 'success_spider')

        # job tag is the same as the spider's
        self.assertEqual(spider_tag, job.tag)

        # has task
        self.assertTrue(target.has_task(node.id))
        # has_task never change the state, always be the same
        self.assertTrue(target.has_task(node.id))

        # get a task
        actual_task = target.get_next_task(node.id)
        self.assertIsNotNone(actual_task)

        # no task
        self.assertFalse(target.has_task(node.id))
        self.assertFalse(target.has_task(node.id))
        self.assertIsNone(target.get_next_task(node.id))


    # agent tags: a, spider tag: None
    def test_a_none(self):
        agent_tags = 'a'
        spider_tag = None
        node = self.node_manager.create_node('*', tags=agent_tags)
        project_name = 'test_project'
        spider_name = 'success_spider'

        self._set_spider_tag(project_name, spider_name, spider_tag)

        target = SchedulerManager()
        job = target.add_task('test_project', 'success_spider')

        # job tag is the same as the spider's
        self.assertEqual(spider_tag, job.tag)

        # has task
        self.assertTrue(target.has_task(node.id))

        # get a task
        actual_task = target.get_next_task(node.id)
        self.assertIsNotNone(actual_task)

    # agent tags: a, spider tag: None
    def test_none_a(self):
        agent_tags = None
        spider_tag = 'a'
        node = self.node_manager.create_node('*', tags=agent_tags)
        project_name = 'test_project'
        spider_name = 'success_spider'

        self._set_spider_tag(project_name, spider_name, spider_tag)

        target = SchedulerManager()
        job = target.add_task('test_project', 'success_spider')

        # job tag is the same as the spider's
        self.assertEqual(spider_tag, job.tag)

        # has task
        self.assertFalse(target.has_task(node.id))
        self.assertFalse(target.has_task(node.id))

        # get a task
        actual_task = target.get_next_task(node.id)
        self.assertIsNone(actual_task)

    def test_a_a(self):
        agent_tags = 'a'
        spider_tag = 'a'
        node = self.node_manager.create_node('*', tags = agent_tags)
        project_name = 'test_project'
        spider_name = 'success_spider'

        self._set_spider_tag(project_name, spider_name, spider_tag)

        target = SchedulerManager()
        # TODO: bad smell SchedulerManager.add_task and .trigger_fired has same action
        job = target.add_task('test_project', 'success_spider')

        # job tag is the same as the spider's
        self.assertEqual(spider_tag, job.tag)

        # has task
        self.assertTrue(target.has_task(node.id))
        # has_task never change the state, always be the same
        self.assertTrue(target.has_task(node.id))

        # get a task
        actual_task = target.get_next_task(node.id)
        self.assertIsNotNone(actual_task)

        # no task
        self.assertFalse(target.has_task(node.id))
        self.assertFalse(target.has_task(node.id))
        self.assertIsNone(target.get_next_task(node.id))

    def test_ab_a(self):
        agent_tags = 'a,b'
        spider_tag = 'a'
        node = self.node_manager.create_node('*', tags = agent_tags)
        project_name = 'test_project'
        spider_name = 'success_spider'

        self._set_spider_tag(project_name, spider_name, spider_tag)

        target = SchedulerManager()
        job = target.add_task('test_project', 'success_spider')

        # job tag is the same as the spider's
        self.assertEqual(spider_tag, job.tag)

        # has task
        self.assertTrue(target.has_task(node.id))
        # has_task never change the state, always be the same
        self.assertTrue(target.has_task(node.id))

        # get a task
        actual_task = target.get_next_task(node.id)
        self.assertIsNotNone(actual_task)

        # no task
        self.assertFalse(target.has_task(node.id))
        self.assertFalse(target.has_task(node.id))
        self.assertIsNone(target.get_next_task(node.id))


    def test_b_a(self):
        agent_tags = 'b'
        spider_tag = 'a'
        node = self.node_manager.create_node('*', tags=agent_tags)
        project_name = 'test_project'
        spider_name = 'success_spider'

        self._set_spider_tag(project_name, spider_name, spider_tag)

        target = SchedulerManager()
        job = target.add_task('test_project', 'success_spider')

        # job tag is the same as the spider's
        self.assertEqual(spider_tag, job.tag)

        # has task
        self.assertFalse(target.has_task(node.id))
        self.assertFalse(target.has_task(node.id))

        # get a task
        actual_task = target.get_next_task(node.id)
        self.assertIsNone(actual_task)


class ScheduleAddTaskTest(ScheduleTest):
    def test_add_task(self):
        target = SchedulerManager()
        project_name = 'test_project'
        spider_name = 'success_spider'
        with session_scope() as session:
            project = session.query(Project).filter(Project.name == project_name).first()
            spider = session.query(Spider).filter(Spider.name == spider_name, Spider.project_id == project.id).first()
            session.query(SpiderExecutionQueue).delete()

        self._set_spider_setting(project_name, spider_name, 'concurrency', 1)

        actual = target.add_task(project_name, spider_name)
        self.assertEqual(actual.spider_id, spider.id)
        self.assertEqual(actual.slot, 1)
        self.assertEqual(actual.project_name, project.name)
        self.assertEqual(actual.spider_name, spider.name)
        self.assertEqual(actual.fire_time.date(), datetime.date.today())
        self.assertEqual(actual.start_time, None)
        self.assertEqual(actual.node_id, None)
        self.assertEqual(actual.status, JOB_STATUS_PENDING)
        self.assertEqual(actual.update_time.date(), datetime.date.today())
        self.assertEqual(actual.pid, None)
        self.assertEqual(actual.tag, None)

        # the persisted
        with session_scope() as session:
            actual2 = session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.id == actual.id).first()

        self.assertEqual(actual.id, actual2.id)
        self.assertEqual(actual.slot, actual2.slot)
        self.assertEqual(actual.project_name, actual2.project_name)
        self.assertEqual(actual.spider_name, actual2.spider_name)
        self.assertEqual(actual.fire_time, actual2.fire_time)
        self.assertEqual(actual.start_time, actual2.start_time)
        self.assertEqual(actual.node_id, actual2.node_id)
        self.assertEqual(actual.status, actual2.status)
        self.assertEqual(actual.update_time, actual2.update_time)
        self.assertEqual(actual.pid, actual2.pid)
        self.assertEqual(actual.tag, actual2.tag)

    def test_add_task_task_already_running(self):
        target = SchedulerManager()
        project_name = 'test_project'
        spider_name = 'success_spider'
        concurrency = 1
        with session_scope() as session:
            #clear jobs
            session.query(SpiderExecutionQueue).delete()

        self._set_spider_setting(project_name, spider_name, 'concurrency', concurrency)

        actual = target.add_task(project_name, spider_name)
        self.assertIsNotNone(actual)

        try:
            actual2 = target.add_task(project_name, spider_name)
            self.fail('Exception not caught')
        except JobRunning:
            pass


    def test_add_task_concurrency(self):
        target = SchedulerManager()
        project_name = 'test_project'
        spider_name = 'success_spider'
        concurrency = 3
        with session_scope() as session:
            #clear jobs
            session.query(SpiderExecutionQueue).delete()

        self._set_spider_setting(project_name, spider_name, 'concurrency', concurrency)

        for i in range(concurrency):
            target.add_task(project_name, spider_name)

        try:
            target.add_task(project_name, spider_name)
            self.fail('Exception not caught')
        except JobRunning:
            pass

        with session_scope() as session:
            existing_jobs = list(session.query(SpiderExecutionQueue))

            for i in range(concurrency):
                # slot begins from 1
                self.assertEqual(existing_jobs[i].slot, i+1)

        self.assertEqual(len(existing_jobs), 3)

