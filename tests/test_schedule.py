import os
import unittest
import logging
import datetime
import json
from tornado.testing import AsyncHTTPTestCase
from six.moves.urllib.parse import urlencode
from scrapydd.models import Session, session_scope, init_database
from scrapydd.models import Project, Spider, HistoricalJob, Node
from scrapydd.models import SpiderExecutionQueue, SpiderSettings
from scrapydd.config import Config
from scrapydd.exceptions import NodeNotFound
from scrapydd.nodes import NodeManager
from scrapydd.storage import ProjectStorage
from scrapydd.poster.encode import multipart_encode
from scrapydd.main import make_app
from scrapydd.schedule import SchedulerManager, JOB_STATUS_PENDING
from scrapydd.schedule import JOB_STATUS_RUNNING, JOB_STATUS_SUCCESS
from scrapydd.schedule import JOB_STATUS_CANCEL, JOB_STATUS_STOPPING
from tests.base import AppTest


LOGGER = logging.getLogger(__name__)


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
        #self._upload_test_project()
        self._delproject()
        self.init_project()


    def _delproject(self):
        postdata = {'project': 'test_project'}
        self.fetch('/delproject.json', method='POST', body=urlencode(postdata))


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
        body = b''.join(datagen)
        self.fetch('/addversion.json', method='POST', headers=headers,
                   body=body)

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
            project = session.query(Project).filter_by(name=project_name).first()
            if project is None:
                project = Project()
                project.name = project_name
                project.storage_version = 2
            project.version = version
            session.add(project)
            session.commit()
            session.refresh(project)

            project_storage = ProjectStorage(self._app.settings.get('project_storage_dir'), project)
            egg_file.seek(0)
            project_storage.put_egg(egg_file, version)

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
        egg_file.close()


class SchedulerManagerTest(unittest.TestCase):
    def setUp(self):
        LOGGER.debug('Clear test database.')
        if os._exists('test.db'):
            os.remove('test.db')
        config = Config(values={'database_url': 'sqlite:///test.db'})
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

    def test_add_task(self):
        project_name = 'test_project'
        spider_name = 'fail_spider'
        target = SchedulerManager()
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            session.query(HistoricalJob).delete()
            session.commit()

            new_job = target.add_task(project_name, spider_name)
            self.assertIsNotNone(new_job)
            self.assertIsNotNone(new_job.spider_id)

            pending_jobs, running_jobs, finished_job = target.jobs(session)
            self.assertEqual([x.id for x in pending_jobs], [new_job.id])
            self.assertEqual([x.id for x in running_jobs], [])
            self.assertEqual([x.id for x in finished_job], [])

    def test_add_task_settings(self):
        project_name = 'test_project'
        spider_name = 'fail_spider'
        origin_settings = {'a': 1, 'b': 'x'}
        target = SchedulerManager()
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()

        new_job = target.add_task(project_name, spider_name,
                                  origin_settings)
        self.assertIsNotNone(new_job)
        self.assertIsNotNone(new_job.spider_id)

        with session_scope() as session:
            loaded_job = session.query(SpiderExecutionQueue)\
                .get(new_job.id)

        self.assertIsNotNone(loaded_job.settings)
        loaded_settings_dict = json.loads(loaded_job.settings)
        self.assertEqual(origin_settings, loaded_settings_dict)


    def test_add_task_twice(self):
        project_name = 'test_project'
        spider_name = 'fail_spider'
        target = SchedulerManager()
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()

        new_job = target.add_task(project_name, spider_name)
        new_job2 = target.add_task(project_name, spider_name)
        self.assertIsNotNone(new_job)
        self.assertIsNotNone(new_job.spider_id)

    def test_get_next_task(self):
        node_id = 1
        project_name = 'test_project'
        spider_name = 'fail_spider'
        target = SchedulerManager()
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            node = Node()
            session.add(node)
            session.flush()
            session.refresh(node)
            node_id = node.id

        new_job = target.add_task(project_name, spider_name)
        next_job = target.get_next_task(node_id)
        next_job2 = target.get_next_task(node_id)
        self.assertEqual(new_job.id, next_job.id)
        self.assertEqual(next_job.node_id, node_id)
        self.assertEqual(next_job.status, JOB_STATUS_RUNNING)
        self.assertIsNone(next_job2)

    def test_get_next_task_concurrent(self):
        node_id = 1
        project_name = 'test_project'
        spider_name = 'fail_spider'
        target = SchedulerManager()
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            node = Node()
            session.add(node)
            session.flush()
            session.refresh(node)
            node_id = node.id

        new_job = target.add_task(project_name, spider_name)
        new_job2 = target.add_task(project_name, spider_name)
        # can get one job
        next_job = target.get_next_task(node_id)
        self.assertEqual(new_job.id, next_job.id)

        # 1 concurrency now, cannot get the second
        next_job2 = target.get_next_task(node_id)
        self.assertIsNone(next_job2)

        # finish one, then can get the second.
        new_job.status = JOB_STATUS_SUCCESS
        target.job_finished(new_job)
        next_job2 = target.get_next_task(node_id)
        self.assertIsNotNone(next_job2)

    def test_get_next_task_no_node(self):
        node_id = 0
        target = SchedulerManager()
        try:
            next_job = target.get_next_task(node_id)
            self.fail('Exception not caught')
        except NodeNotFound:
            pass

    def test_finish_job_success(self):
        project_name = 'test_project'
        spider_name = 'fail_spider'
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            node = Node()
            session.add(node)
            session.flush()
            session.refresh(node)
            node_id = node.id

        target = SchedulerManager()
        new_job = target.add_task(project_name, spider_name)
        new_job2 = target.add_task(project_name, spider_name)

        next_job = target.get_next_task(node_id)

        next_job.status = JOB_STATUS_SUCCESS
        target.job_finished(next_job)
        with session_scope() as session:
            queue_job = session.query(SpiderExecutionQueue)\
                .get(next_job.id)
            self.assertIsNone(queue_job)
            history_job = session.query(HistoricalJob).get(next_job.id)
            self.assertIsNotNone(history_job)

        next_job2 = target.get_next_task(node_id)
        self.assertIsNotNone(next_job2)
        self.assertEqual(next_job2.id, new_job2.id)

    def test_finish_job_success(self):
        project_name = 'test_project'
        spider_name = 'fail_spider'
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            node = Node()
            session.add(node)
            session.flush()
            session.refresh(node)
            node_id = node.id

        target = SchedulerManager()
        new_job = target.add_task(project_name, spider_name)
        new_job2 = target.add_task(project_name, spider_name)

        next_job = target.get_next_task(node_id)

        next_job.status = JOB_STATUS_SUCCESS
        target.job_finished(next_job)
        with session_scope() as session:
            queue_job = session.query(SpiderExecutionQueue)\
                .get(next_job.id)
            self.assertIsNone(queue_job)
            history_job = session.query(HistoricalJob).get(next_job.id)
            self.assertIsNotNone(history_job)
            self.assertEqual(history_job.status, JOB_STATUS_SUCCESS)

        next_job2 = target.get_next_task(node_id)
        self.assertIsNotNone(next_job2)
        self.assertEqual(next_job2.id, new_job2.id)

    def test_cancel_job_pending(self):
        project_name = 'test_project'
        spider_name = 'fail_spider'
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            node = Node()
            session.add(node)
            session.flush()
            session.refresh(node)
            node_id = node.id

        target = SchedulerManager()
        new_job = target.add_task(project_name, spider_name)

        target.cancel_task(new_job.id)
        with session_scope() as session:
            queue_job = session.query(SpiderExecutionQueue)\
                .get(new_job.id)
            self.assertIsNone(queue_job)
            history_job = session.query(HistoricalJob).get(new_job.id)
            self.assertIsNotNone(history_job)
            self.assertEqual(history_job.status, JOB_STATUS_CANCEL)

    def test_cancel_job_running(self):
        project_name = 'test_project'
        spider_name = 'fail_spider'
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            node = Node()
            session.add(node)
            session.flush()
            session.refresh(node)
            node_id = node.id

        target = SchedulerManager()
        new_job = target.add_task(project_name, spider_name)
        next_job = target.get_next_task(node_id)
        self.assertEqual(next_job.status, JOB_STATUS_RUNNING)
        target.cancel_task(next_job.id)
        with session_scope() as session:
            queue_job = session.query(SpiderExecutionQueue)\
                .get(next_job.id)
            self.assertIsNone(queue_job)
            history_job = session.query(HistoricalJob).get(next_job.id)
            self.assertIsNotNone(history_job)
            self.assertEqual(history_job.status, JOB_STATUS_CANCEL)

    def test_jobs_running(self):
        project_name = 'test_project'
        spider_name = 'fail_spider'
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            node = Node()
            node2 = Node()
            session.add(node)
            session.add(node2)
            session.flush()
            session.refresh(node)
            session.refresh(node2)
            node_id = node.id
            node_id_2 = node2.id

        target = SchedulerManager()
        running_ids = []
        to_kill = list(target.jobs_running(node_id, running_ids))
        self.assertEqual(to_kill, [])

        target.add_task(project_name, spider_name)
        next_job = target.get_next_task(node_id)
        running_ids = [next_job.id]

        update_time = datetime.datetime.now()
        target._now = lambda: update_time
        to_kill = list(target.jobs_running(node_id, running_ids))
        with session_scope() as session:
            running_job = session.query(SpiderExecutionQueue)\
                .get(next_job.id)
        self.assertEqual(running_job.update_time, update_time)
        self.assertEqual(to_kill, [])

        # test other node_id
        to_kill = list(target.jobs_running(node_id_2, running_ids))
        self.assertEqual(to_kill, [next_job.id])

        target.cancel_task(next_job.id)
        to_kill = list(target.jobs_running(node_id, [next_job.id]))
        self.assertEqual(to_kill, [next_job.id])

    def test_on_node_expire(self):
        project_name = 'test_project'
        spider_name = 'fail_spider'
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            node = Node()
            session.add(node)
            session.flush()
            session.refresh(node)
            node_id = node.id

        target = SchedulerManager()
        target.add_task(project_name, spider_name)
        next_job = target.get_next_task(node_id)
        target.on_node_expired(node_id)
        with session_scope() as session:
            running_job = session.query(SpiderExecutionQueue)\
                .get(next_job.id)

        self.assertEqual(running_job.status, JOB_STATUS_PENDING)
        self.assertEqual(running_job.node_id, None)

    def test_reset_timeout_job_start_time_expire(self):
        project_name = 'test_project'
        spider_name = 'fail_spider'
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            node = Node()
            session.add(node)
            session.flush()
            session.refresh(node)
            node_id = node.id

        start_time = datetime.datetime.now()
        still_running_time = start_time + datetime.timedelta(seconds=3599)
        expire_time = start_time + datetime.timedelta(seconds=3601)

        target = SchedulerManager()
        target.add_task(project_name, spider_name)
        target._now = lambda: start_time
        next_job = target.get_next_task(node_id)
        with session_scope() as session:
            running_job = session.query(SpiderExecutionQueue) \
                .get(next_job.id)
        self.assertEqual(running_job.start_time, start_time)
        self.assertEqual(running_job.update_time, start_time)
        self.assertEqual(running_job.status, JOB_STATUS_RUNNING)
        self.assertEqual(running_job.node_id, node_id)

        target._now = lambda: still_running_time
        # update the update_time, to avoid update_time expire first.
        list(target.jobs_running(node_id, [running_job.id]))
        target.reset_timeout_job()
        with session_scope() as session:
            running_job = session.query(SpiderExecutionQueue) \
                .get(next_job.id)
        self.assertEqual(running_job.start_time, start_time)
        self.assertEqual(running_job.update_time, still_running_time)
        self.assertEqual(running_job.status, JOB_STATUS_RUNNING)
        self.assertEqual(running_job.node_id, node_id)

        target._now = lambda: expire_time
        target.reset_timeout_job()
        with session_scope() as session:
            expired_job = session.query(SpiderExecutionQueue) \
                .get(next_job.id)
        self.assertEqual(expired_job.status, JOB_STATUS_STOPPING)

    def test_reset_timeout_job_update_timeout(self):
        project_name = 'test_project'
        spider_name = 'fail_spider'
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            node = Node()
            session.add(node)
            session.flush()
            session.refresh(node)
            node_id = node.id

        start_time = datetime.datetime.now()
        still_running_time = start_time + datetime.timedelta(seconds=59)
        expire_time = start_time + datetime.timedelta(seconds=61)

        target = SchedulerManager()
        target.add_task(project_name, spider_name)
        target._now = lambda: start_time
        next_job = target.get_next_task(node_id)
        with session_scope() as session:
            running_job = session.query(SpiderExecutionQueue) \
                .get(next_job.id)
        self.assertEqual(running_job.start_time, start_time)
        self.assertEqual(running_job.update_time, start_time)
        self.assertEqual(running_job.status, JOB_STATUS_RUNNING)
        self.assertEqual(running_job.node_id, node_id)

        target._now = lambda: still_running_time
        # update the update_time, to avoid update_time expire first.
        target.reset_timeout_job()
        with session_scope() as session:
            running_job = session.query(SpiderExecutionQueue) \
                .get(next_job.id)
        self.assertEqual(running_job.start_time, start_time)
        self.assertEqual(running_job.status, JOB_STATUS_RUNNING)
        self.assertEqual(running_job.node_id, node_id)

        target._now = lambda: expire_time
        target.reset_timeout_job()
        with session_scope() as session:
            expired_job = session.query(SpiderExecutionQueue) \
                .get(next_job.id)
        self.assertEqual(expired_job.status, JOB_STATUS_PENDING)


class ScheduleTagTest(AppTest):
    def setUp(self):
        super(ScheduleTagTest, self).setUp()
        with session_scope() as session:
           session.query(SpiderExecutionQueue).delete()

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

    def _remove_spider_tag(self, project_name, spider_name):
        with session_scope() as session:
            project = session.query(Project).filter(Project.name == project_name).first()
            spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider_name).first()
            setting_tag = session.query(SpiderSettings).filter_by(spider_id=spider.id,
                                                                  setting_key='tag').first()
            if setting_tag:
                session.delete(setting_tag)

    def get_app(self):
        config = Config()
        self.scheduler_manager = scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        self.node_manager = node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        return make_app(scheduler_manager, node_manager, None)

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
        self.assertFalse(target.has_task(node.id))

        # get a task
        actual_task = target.get_next_task(node.id)
        self.assertIsNone(actual_task)

    # agent tags: a, spider tag: None
    def test_none_a(self):
        agent_tags = None
        spider_tag = 'a'
        node = self.node_manager.create_node('*', tags=agent_tags)
        project_name = 'test_project'
        spider_name = 'success_spider'

        self._set_spider_tag(project_name, spider_name, spider_tag)
        #self._remove_spider_tag(project_name, spider_name)

        target = SchedulerManager()
        job = target.add_task('test_project', 'success_spider')

        # job tag is the same as the spider's
        #self.assertEqual(spider_tag, job.tag)

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
