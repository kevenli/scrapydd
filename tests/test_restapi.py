from tornado.testing import AsyncHTTPTestCase
import json
from scrapydd.main import *
from six.moves.urllib_parse import urlencode, urlparse
from scrapydd.security import HmacAuthorize, generate_digest, authenticated_request
from tornado.httpclient import HTTPRequest
from poster.encode import multipart_encode
from scrapydd.security import encrypt_password
from w3lib.http import basic_auth_header


class AppTest(AsyncHTTPTestCase):
    @classmethod
    def setUpClass(cls):
        if os._exists('test.db'):
            os.remove('test.db')
        config = Config(values = {'database_url': 'sqlite:///test.db'})
        init_database(config)
        os.environ['ASYNC_TEST_TIMEOUT'] = '120'

    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        return make_app(scheduler_manager, node_manager, None, secret_key='123')

    def fetch_request(self, request, **kwargs):
        self.http_client.fetch(request, self.stop, **kwargs)
        return self.wait()

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


class SecureAppTest(AppTest):
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

        return make_app(scheduler_manager, node_manager, None, secret_key='123', enable_authentication=True)

    def populate_authorization_header(self, headers):
        headers['Authorization'] = basic_auth_header('admin', 'password')

    def _upload_test_project(self):
        # upload a project

        post_data = {}
        post_data['egg'] = open(os.path.join(os.path.dirname(__file__), 'test_project-1.0-py2.7.egg'), 'rb')
        post_data['project'] = 'test_project'
        post_data['version'] = '1.0'

        datagen, headers = multipart_encode(post_data)
        self.populate_authorization_header(headers)
        databuffer = ''.join(datagen)
        response = self.fetch('/addversion.json', method='POST', headers=headers, body=databuffer)
        self.assertEqual(200, response.code)


class ScheduleTest(AppTest):
    def test_schedule(self):
        self._upload_test_project()
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            session.commit()

        post_params = {
            'project' : 'test_project',
            'spider': 'success_spider'
        }
        response = self.fetch('/schedule.json', method="POST", body=urlencode(post_params))
        self.assertEqual(200, response.code)


class SecureScheduleTest(SecureAppTest):
    def test_schedule_without_auth(self):
        self._upload_test_project()
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            session.commit()

        post_params = {
            'project' : 'test_project',
            'spider': 'success_spider'
        }
        response = self.fetch('/schedule.json', method="POST", body=urlencode(post_params))
        self.assertEqual(403, response.code)

    def test_schedule_with_auth(self):
        self._upload_test_project()
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            session.commit()

        post_params = {
            'project': 'test_project',
            'spider': 'success_spider'
        }
        headers = {}
        self.populate_authorization_header(headers)
        response = self.fetch('/schedule.json', method="POST", body=urlencode(post_params), headers=headers)
        self.assertEqual(200, response.code)


class NodesHandlerTest(AppTest):
    def test_register(self):
        response = self.fetch('/api/v1/nodes', method="POST", body="")
        self.assertEqual(200, response.code)

        response_data = json.loads(response.body)
        new_node_id = response_data['id']

        with session_scope() as session:
            new_node = session.query(Node).filter_by(id=new_node_id).first()

        self.assertEqual('127.0.0.1', new_node.client_ip)
        self.assertEqual(datetime.date.today(), new_node.create_time.date())
        self.assertEqual(datetime.date.today(), new_node.last_heartbeat.date())
        self.assertEqual(True, new_node.isalive)
        self.assertEqual(None, new_node.tags)


class GetNextJobTest(AppTest):
    def test_next_job(self):
        post_data = {'node_id': '1'}
        body = urlencode(post_data)
        response = self.fetch('/api/v1/jobs/next', method="POST", body=body)
        self.assertEqual(200, response.code)


class GetNextJobSecureTest(AppTest):
    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()

        self.appkey = 'abc'
        self.appsecret = '123'
        self.node_id = node_manager.create_node('127.0.0.1').id

        with session_scope() as session:
            existing_key = session.query(UserKey).filter_by(app_key=self.appkey).first()
            if not existing_key:
                userkey = UserKey(user_id=1, app_key=self.appkey, app_secret=self.appsecret)
                session.add(userkey)
                session.commit()


        return make_app(scheduler_manager, node_manager, enable_authentication=True)


    def test_next_job(self):
        post_data = {'node_id': self.node_id}
        body = urlencode(post_data)
        path = '/api/v1/jobs/next'
        request = authenticated_request(url=self.get_url(path), method='POST', body=body, app_key=self.appkey,
                                        app_secret=self.appsecret)
        response = self.fetch_request(request)
        self.assertEqual(200, response.code)

    def test_next_job_wrong_app_key(self):
        post_data = {'node_id': self.node_id}
        body = urlencode(post_data)
        path = '/api/v1/jobs/next'
        request = authenticated_request(url=self.get_url(path), method='POST', body=body, app_key='somethingwrong',
                                        app_secret=self.appsecret)
        response = self.fetch_request(request)
        self.assertEqual(403, response.code)

    def test_next_job_wrong_app_secret(self):
        post_data = {'node_id': self.node_id}
        body = urlencode(post_data)
        path = '/api/v1/jobs/next'
        request = authenticated_request(url=self.get_url(path), method='POST', body=body, app_key=self.appkey,
                                        app_secret='somethingwrong')
        response = self.fetch_request(request)
        self.assertEqual(403, response.code)