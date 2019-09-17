from ..base import AppTest
from scrapydd.models import session_scope, Node, SpiderExecutionQueue, HistoricalJob
import datetime
from six.moves.urllib.parse import urlencode
from tornado.httpclient import HTTPRequest
import unittest
import json
from scrapydd.poster.encode import multipart_encode
from six import BytesIO
from scrapydd.main import make_app
from scrapydd.webhook import WebhookDaemon
from scrapydd.settting import SpiderSettingLoader
from scrapydd.schedule import SchedulerManager
from scrapydd.nodes import NodeManager
from scrapydd.config import Config
from scrapydd.security import generate_digest


class NodeTest(AppTest):
    def register_node(self):
        with session_scope() as session:
            session.query(Node).delete()

        response = self.fetch('/nodes', method="POST", body="")
        self.assertEqual(200, response.code)
        return json.loads(response.body)['id']


class NodeSecuryTest(AppTest):
    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        webhook_daemon = WebhookDaemon(config, SpiderSettingLoader())
        webhook_daemon.init()
        return make_app(scheduler_manager, node_manager, webhook_daemon, secret_key='123', enable_node_registration=True)

    def fetch(self, path, **kwargs):
        auth = kwargs.pop('auth', True)
        method = kwargs.pop('method', 'GET')
        body = kwargs.pop('body', b'')
        headers = kwargs.pop('headers', {})
        if auth:
            digest = generate_digest(self.node_key.secret_key, method, path, body)
            headers['Authorization'] = b'%s %s %s' % (b'HMAC', self.node_key.key, digest)

        return super(NodeSecuryTest, self).fetch(path, method=method, body=body, headers=headers)


class NodesHandlerTest(AppTest):
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


class GetNextJobTest(NodeTest):
    def test_next_job(self):
        node_id = self.register_node()
        post_data = {'node_id': node_id}
        headers = {'X-Dd-Nodeid' : str(node_id)}
        body = urlencode(post_data)
        path = '/executing/next_task'
        response = self.fetch(path, method='POST', body=body, headers=headers)
        self.assertEqual(200, response.code)


@unittest.skip
class GetNextJobSecureTest(NodeTest):
    def test_next_job_wrong_app_key(self):
        node_id = self.register_node()
        post_data = {'node_id': node_id}
        body = urlencode(post_data)
        path = '/api/v1/jobs/next'
        request = HTTPRequest(url=self.get_url(path), method='POST', body=body, app_key='somethingwrong',
                                        app_secret=self.appsecret)
        response = self.fetch(request)
        self.assertEqual(403, response.code)

    def test_next_job_wrong_app_secret(self):
        node_id = self.register_node()
        post_data = {'node_id': node_id}
        body = urlencode(post_data)
        path = '/api/v1/jobs/next'
        request = HTTPRequest(url=self.get_url(path), method='POST', body=body, app_key=self.appkey,
                                        app_secret='somethingwrong')
        response = self.fetch(request)
        self.assertEqual(403, response.code)


class ExecuteCompleteHandlerTest(NodeTest):
    def test_job_complete(self):
        project_name = 'test_project'
        spider_name = 'success_spider'

        node_id = self.register_node()

        # schedule a job
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            session.commit()
        run_spider_post_data =  {'project': project_name, 'spider': spider_name}
        res = self.fetch('/schedule.json', method='POST', body=urlencode(run_spider_post_data))
        self.assertEqual(200, res.code)

        # fetch a job
        next_job_post_data = {'node_id': node_id}
        headers = {'X-Dd-Nodeid': str(node_id)}
        res = self.fetch('/executing/next_task', method='POST', body=urlencode(next_job_post_data), headers=headers)
        self.assertEqual(200, res.code)
        task_id = json.loads(res.body)['data']['task']['task_id']

        # job start
        post_data = {'pid' : '1'}
        res = self.fetch('/jobs/%s/start' % task_id, method='POST', body=urlencode(post_data))
        self.assertEqual(200, res.code)

        # complete this job
        post_data = {'task_id': task_id,
                     'status': 'success'}
        post_data['log'] = BytesIO(b'some logs')
        post_data['items'] = BytesIO(b'{"a" : "some items"}')
        datagen, headers = multipart_encode(post_data)
        headers['X-Dd-Nodeid'] = str(node_id)
        res = self.fetch('/executing/complete', method='POST', headers=headers, body=b''.join(datagen))
        self.assertEqual(200, res.code)

        with session_scope() as session:
            complete_job = session.query(HistoricalJob).filter_by(id=task_id).first()
            self.assertIsNotNone(complete_job)
            self.assertEqual(2, complete_job.status)


class NodeHeartbeatHandler(NodeTest):
    def test_heartbeat(self):
        node_id = self.register_node()
        headers = {'X-Dd-Nodeid': str(node_id)}
        res = self.fetch('/nodes/%d/heartbeat' % node_id, method='POST', body=b'', headers=headers)
        self.assertEqual(200, res.code)


class NodesHandlerSecureTest(NodeSecuryTest, NodesHandlerTest):
    def post_without_auth(self):
        response = self.fetch('/nodes', method="POST", body="", auth=False)
        self.assertEqual(403, response.code)