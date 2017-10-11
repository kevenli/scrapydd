from unittest import TestCase
from scrapydd.main import *
from scrapydd.config import Config
from tornado.testing import AsyncHTTPTestCase
from poster.encode import multipart_encode
from scrapydd.models import init_database
import os.path
import urllib

class MainTest(AsyncHTTPTestCase):
    @classmethod
    def setUpClass(cls):
        if os._exists('test.db'):
            os.remove('test.db')
        config = Config(values = {'database_url': 'sqlite:///test.db'})
        init_database(config)
        os.environ['ASYNC_TEST_TIMEOUT'] = '30'

    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        return make_app(scheduler_manager, node_manager, None)

    def _delproject(self):
        postdata = {'project': 'test_project'}
        self.fetch('/delproject.json', method='POST', body=urllib.urlencode(postdata))

    def _upload_test_project(self):
        # upload a project

        post_data = {}
        post_data['egg'] = open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = 'test_project'
        post_data['version'] = '1.0'

        datagen, headers = multipart_encode(post_data)
        databuffer = ''.join(datagen)
        self.fetch('/addversion.json', method='POST', headers=headers, body=databuffer)


class UploadTest(MainTest):
    def test_logging_init(self):
        self.skipTest('no logging init')

    def test_MainHandler(self):
        response = self.fetch('/')
        self.assertEqual(200, response.code)

    def test_UploadProject_post(self):
        os.environ['ASYNC_TEST_TIMEOUT'] = '30'
        post_data = {}
        post_data['egg'] = open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = 'test_project'
        post_data['version'] = '1.0'

        datagen, headers = multipart_encode(post_data)
        databuffer = ''.join(datagen)
        response = self.fetch('/addversion.json', method='POST', headers=headers, body=databuffer)

        self.assertEqual(200, response.code)

    def test_UploadProject_get(self):
        response = self.fetch('/addversion.json')
        self.assertEqual(200, response.code)

class ScheduleHandlerTest(MainTest):
    def setUp(self):
        super(ScheduleHandlerTest, self).setUp()
        self._delproject()
        self._upload_test_project()
        logger.info('setup')

    def test_post(self):
        # schedule once
        project = 'test_project'
        spider = 'success_spider'
        postdata = urllib.urlencode({
            'project': project,
            'spider': spider
        })
        response = self.fetch('/schedule.json', method='POST', body=postdata)
        self.assertEqual(200, response.code)

    def test_post_job_already_running(self):
        project = 'test_project'
        spider = 'success_spider'
        postdata = urllib.urlencode({
            'project': project,
            'spider': spider
        })
        self.fetch('/schedule.json', method='POST', body=postdata)
        response = self.fetch('/schedule.json', method='POST', body=postdata)
        self.assertEqual(400, response.code)
        self.assertIn('job is running', response.body)


class AddScheduleHandlerTest(MainTest):
    def setUp(self):
        super(AddScheduleHandlerTest, self).setUp()
        self._delproject()
        self._upload_test_project()

    def test_add_scheduler(self):
        project = 'test_project'
        spider = 'success_spider'
        cron = '* * * * *'

        postdata = {
            'project':project,
            'spider':spider,
            'cron':cron
        }

        response = self.fetch('/add_schedule.json', method='POST', body=urllib.urlencode(postdata))
        self.assertEqual(200, response.code)
        self.assertIn('ok', response.body)


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


class SpiderInstanceHandler2Test(MainTest):
    def setUp(self):
        super(SpiderInstanceHandler2Test, self).setUp()
        self._upload_test_project()

    def test_get(self):
        with session_scope() as session:
            spider = session.query(Spider).first()
            project = spider.project

        self.assertIsNotNone(spider)
        response = self.fetch('/projects/%s/spiders/%s' % (project.name, spider.name))
        self.assertEqual(200, response.code)


class SpiderEggHandlerTest(MainTest):
    def setUp(self):
        super(SpiderEggHandlerTest, self).setUp()
        self._upload_test_project()

    def test_get(self):
        with session_scope() as session:
            spider = session.query(Spider).first()
            project = spider.project

        self.assertIsNotNone(spider)
        # TODO: this interface should be replaced by /projects/xxx/spiders/yyy/egg
        response = self.fetch('/spiders/%d/egg' % (spider.id, ))
        self.assertEqual(200, response.code)
