import uuid
import unittest
import json
import datetime
from six import ensure_binary
from six.moves.urllib.parse import urlencode
from six import BytesIO
from tornado.httpclient import HTTPRequest
from scrapydd.models import session_scope, Node, SpiderExecutionQueue, HistoricalJob, NodeKey
from scrapydd.poster.encode import multipart_encode
from scrapydd.main import make_app
from scrapydd.webhook import WebhookDaemon
from scrapydd.settting import SpiderSettingLoader
from scrapydd.schedule import SchedulerManager
from scrapydd.nodes import NodeManager
from scrapydd.config import Config
from scrapydd.security import generate_digest, generate_random_string
from scrapydd.stream import MultipartRequestBodyProducer
from tests.base import AppTest


class NodeTest(AppTest):
    def register_node(self):
        with session_scope() as session:
            session.query(Node).delete()

        response = self.fetch('/nodes', method="POST", body="")
        self.assertEqual(200, response.code)
        return json.loads(response.body)['id']

class NodeSecureTest(NodeTest):
    def setUp(self):
        super(NodeSecureTest, self).setUp()
        with session_scope() as session:
            node = self.node_manager.node_online(session, None, None, None)

            nodekey = NodeKey()
            nodekey.key = str(uuid.uuid4())
            nodekey.create_at = datetime.datetime.now()
            nodekey.secret_key = generate_random_string(32)
            session.add(nodekey)
            session.commit()
            self.node_key = nodekey
            self.node_id = node.id

    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        webhook_daemon = WebhookDaemon(config, SpiderSettingLoader(), scheduler_manager)
        webhook_daemon.init()
        self.node_manager = node_manager
        return make_app(scheduler_manager, node_manager, webhook_daemon, secret_key='123',
                        enable_node_registration=True)

    def register_node(self):
        node_key = self.node_manager.create_node_key()
        path = '/nodes/register'
        method = 'POST'
        query = ''
        body = ''
        headers = {'Authorization': '%s %s %s' % ('HMAC',
                                                  node_key.key,
                                                  generate_digest(node_key.secret_key, method, path, query, body))}
        response = self.fetch(path, method=method, body=body, headers=headers)
        self.assertEqual(200, response.code)
        self.node_key = node_key
        new_node_id = json.loads(response.body)['id']
        return new_node_id

    def fetch_secure(self, path, **kwargs):
        auth = kwargs.pop('auth', True)
        method = kwargs.pop('method', 'GET')
        headers = kwargs.pop('headers', {})
        body = kwargs.pop('body', None)
        if auth:
            body_binary = ensure_binary(body) if body else b''
            digest = generate_digest(self.node_key.secret_key, method, path, '', body_binary)
            key = ensure_binary(self.node_key.key)
            digest = ensure_binary(digest)
            headers['Authorization'] = b'%s %s %s' % (b'HMAC', key, digest)

        return super(NodeSecureTest, self).fetch(path, method=method, body=body, headers=headers, **kwargs)


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


    def test_register_realip(self):
        with session_scope() as session:
            session.query(Node).delete()

        headers = {'X-Real-IP': '1.2.3.4'}
        response = self.fetch('/nodes',
                              method="POST", headers=headers, body="")

        with session_scope() as session:
            new_node = session.query(Node).first()

        self.assertEqual(200, response.code)
        self.assertEqual('1.2.3.4', new_node.client_ip)
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
        res = self.fetch('/executing/next_task', method='POST', body=urlencode(next_job_post_data),
                         headers=headers)
        self.assertEqual(200, res.code)
        task_id = json.loads(res.body)['data']['task']['task_id']

        # job start
        post_data = {'pid' : '1'}
        headers = {'X-Dd-Nodeid': str(node_id)}
        res = self.fetch('/jobs/%s/start' % task_id, method='POST', headers=headers, body=urlencode(post_data))
        self.assertEqual(200, res.code)

        # complete this job
        post_data = {'task_id': task_id,
                     'status': 'success'}
        post_data['log'] = BytesIO(b'some logs')
        post_data['items'] = BytesIO(b'{"a" : "some items"}')
        datagen, headers = multipart_encode(post_data)
        headers['X-Dd-Nodeid'] = str(node_id)
        #
        res = self.fetch('/executing/complete', method='POST', headers=headers,
                         body_producer=MultipartRequestBodyProducer(datagen))
        self.assertEqual(200, res.code)

        with session_scope() as session:
            complete_job = session.query(HistoricalJob).filter_by(id=task_id).first()
            self.assertIsNotNone(complete_job)
            self.assertEqual(2, complete_job.status)


class ExecuteCompleteHandlerSecureTest(NodeSecureTest):
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
        res = self.fetch_secure('/executing/next_task', method='POST', body=urlencode(next_job_post_data),
                         headers=headers)
        self.assertEqual(200, res.code)
        task_id = json.loads(res.body)['data']['task']['task_id']

        # job start
        post_data = {'pid' : '1'}
        headers = {'X-Dd-Nodeid': str(node_id)}
        res = self.fetch_secure('/jobs/%s/start' % task_id, method='POST', headers=headers, body=urlencode(post_data))
        self.assertEqual(200, res.code)

        # complete this job
        post_data = {'task_id': task_id,
                     'status': 'success'}
        post_data['log'] = BytesIO(b'some logs')
        post_data['items'] = BytesIO(b'{"a" : "some items"}')
        datagen, headers = multipart_encode(post_data)
        headers['X-Dd-Nodeid'] = str(node_id)
        res = self.fetch_secure('/executing/complete', method='POST', headers=headers,
                                body_producer=MultipartRequestBodyProducer(datagen))
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


class NodesHandlerSecureTest(NodeSecureTest):
    def test_post_new_node_with_key(self):
        node_key = self.node_manager.create_node_key()
        headers = {'Authorization': '%s %s %s' % ('HMAC',
                                                  node_key.key,
                                                  generate_digest(node_key.secret_key, 'POST', '/nodes', '', ''))}
        response = self.fetch('/nodes', method="POST", body="", headers=headers)
        self.assertEqual(403, response.code)

    def test_post_exist_node_with_key(self):
        self.node_key = self.node_manager.create_node_key()
        res = self.fetch_secure('/nodes/register', method='POST', body='')
        self.assertEqual(200, res.code)
        node_id = json.loads(res.body)['id']


        headers = {'Authorization': '%s %s %s' % ('HMAC',
                                                  self.node_key.key,
                                                  generate_digest(self.node_key.secret_key, 'POST', '/nodes', '', ''))}
        response = self.fetch('/nodes', method="POST", body="", headers=headers)
        self.assertEqual(200, response.code)
        new_node_id = json.loads(response.body)['id']
        self.assertTrue(new_node_id > 0)

    def test_post_without_auth(self):
        response = self.fetch('/nodes', method="POST", body="")
        self.assertEqual(403, response.code)

    def test_post_real_ip(self):
        self.node_key = self.node_manager.create_node_key()
        res = self.fetch_secure('/nodes/register', method='POST', body='')
        self.assertEqual(200, res.code)
        node_id = json.loads(res.body)['id']


        headers = {'Authorization': '%s %s %s' % ('HMAC',
                                                  self.node_key.key,
                                                  generate_digest(self.node_key.secret_key, 'POST', '/nodes', '', ''))}
        headers['X-Real-IP'] = '1.2.3.4'
        response = self.fetch('/nodes', method="POST", body="", headers=headers)
        self.assertEqual(200, response.code)
        new_node_id = json.loads(response.body)['id']
        self.assertTrue(new_node_id > 0)
        with session_scope() as session:
            active_node = session.query(Node).get(new_node_id)
            self.assertEqual(active_node.client_ip, '1.2.3.4')


class RegisterNodeHandlerSecureTest(NodeSecureTest):
    def test_post(self):
        node_key = self.node_manager.create_node_key()
        headers = {'Authorization': '%s %s %s' % ('HMAC',
                                                  node_key.key,
                                                  generate_digest(node_key.secret_key, 'POST', '/nodes/register', '', ''))}
        response = self.fetch('/nodes/register', method="POST", body="",
                              headers=headers)
        self.assertEqual(200, response.code)
        new_node_id = json.loads(response.body)['id']
        self.assertTrue(new_node_id > 0)
        with session_scope() as session:
            node = session.query(Node).get(new_node_id)
            self.assertEqual(node.node_key_id, node_key.id)

            updated_node_key = session.query(NodeKey).get(node_key.id)
            self.assertEqual(updated_node_key.used_node_id, new_node_id)

    def test_post_get_real_ip(self):
        node_key = self.node_manager.create_node_key()
        headers = {'Authorization': '%s %s %s' % ('HMAC',
                                                  node_key.key,
                                                  generate_digest(node_key.secret_key, 'POST', '/nodes/register', '',
                                                                  ''))}
        headers['x-real-ip'] = '1.2.3.4'
        response = self.fetch('/nodes/register', method="POST", body="",
                              headers=headers)
        self.assertEqual(200, response.code)
        new_node_id = json.loads(response.body)['id']
        self.assertTrue(new_node_id > 0)
        with session_scope() as session:
            node = session.query(Node).get(new_node_id)
            self.assertEqual(node.node_key_id, node_key.id)
            self.assertEqual(node.client_ip, '1.2.3.4')

            updated_node_key = session.query(NodeKey).get(node_key.id)
            self.assertEqual(updated_node_key.used_node_id, new_node_id)


class JobEggHandlerTest(NodeSecureTest):
    def test_get(self):
        project_name = 'test_project'
        spider_name = 'success_spider'

        node_id = self.register_node()

        # schedule a job
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            session.commit()
        run_spider_post_data = {'project': project_name,
                                'spider': spider_name}
        res = self.fetch('/schedule.json', method='POST',
                         body=urlencode(run_spider_post_data))
        self.assertEqual(200, res.code)

        # fetch a job
        next_job_post_data = {'node_id': node_id}
        headers = {'X-Dd-Nodeid': str(node_id)}
        res = self.fetch_secure('/executing/next_task', method='POST',
                                body=urlencode(next_job_post_data),
                                headers=headers)
        self.assertEqual(200, res.code)
        task_id = json.loads(res.body)['data']['task']['task_id']
        res = self.fetch_secure(f'/jobs/{task_id}/egg', method='GET',
                                headers=headers)
        self.assertEqual(200, res.code)


class CreateNodeSessionHandlerTest(NodeTest):
    def test_post(self):
        res = self.fetch('/v1/nodeSessions', method='POST', body='')
        self.assertEqual(200, res.code)
        res_data = json.loads(res.body)
        self.assertIsNotNone(res_data['id'])
        self.assertIsNotNone(res_data['node_id'])

    def test_post_body_tags(self):
        post_data = {
        }
        body = urlencode(post_data, doseq=True)
        res = self.fetch('/v1/nodeSessions', method='POST', body=body)
        self.assertEqual(200, res.code)
        res_data = json.loads(res.body)
        node_id = res_data['node_id']
        session_id = res_data['id']

        self.assertTrue(node_id > 0)
        self.assertTrue(session_id > 0)

        #headers = {'X-Dd-Nodeid': str(node_id)}
        #res = self.fetch('/v1/nodes/%s' % node_id, headers=headers)
        res = self.fetch('/v1/nodes/%s' % node_id)
        self.assertEqual(200, res.code)
        res_data = json.loads(res.body)


class NodeSessionInstanceHeartbeatHandlerTest(NodeTest):
    def session_login(self):
        post_data = {
            'tags': ''
        }
        res = self.fetch('/v1/nodeSessions', method='POST',
                         body=urlencode(post_data))
        self.assertEqual(200, res.code)
        res_data = json.loads(res.body)
        self.node_id = res_data['node_id']
        self.session_id = res_data['id']

    def test_post(self):
        post_data = {}
        self.session_login()
        headers = {'X-Dd-Nodeid': str(self.node_id)}
        res = self.fetch('/v1/nodeSessions/%s:heartbeat' % self.session_id,
                         method='POST', body=urlencode(post_data),
                         headers=headers,
                         allow_nonstandard_methods=True)
        self.assertEqual(200, res.code)
        res_data = json.loads(res.body)
        self.assertEqual(res_data['kill_job_ids'], [])
        self.assertEqual(res_data['new_job_available'], False)

    def test_post_running_jobs(self):
        job_ids = ['1', '2']
        post_data = {
            'running_job_ids': job_ids
        }
        self.session_login()
        res = self.fetch('/v1/nodeSessions/%s:heartbeat' % self.session_id,
                         method='POST', body=urlencode(post_data, doseq=True),
                         allow_nonstandard_methods=True)
        self.assertEqual(200, res.code)
        res_data = json.loads(res.body)
        self.assertEqual(res_data['kill_job_ids'], job_ids)
        self.assertEqual(res_data['new_job_available'], False)


class NodeCollectionHandlerTest(NodeTest):
    def test_post(self):
        new_key = self._app.settings.get('node_manager').create_node_key()

        # pass parameters other than body by query parameter
        # according to api design guide
        # https://cloud.google.com/apis/design/standard_methods#create
        res = self.fetch('/v1/nodes' + '?node_key=' + new_key.key,
                         method='POST',
                         body='')
        self.assertEqual(200, res.code)

        res_data = json.loads(res.body)

        self.assertIsNotNone(res_data['name'])
        self.assertIsNotNone(res_data['id'])
        self.assertIsNotNone(res_data['display_name'])
        self.assertIsNotNone(res_data['tags'])
        self.assertEqual(len(res_data['tags']), 0)
        self.assertIsNotNone(res_data['is_online'])
        self.assertIsNotNone(res_data['client_ip'])

    def test_post_full(self):
        new_key = self._app.settings.get('node_manager').create_node_key()
        post_data = {
            'tags': ['a', 'bb', 'ccc']
        }

        res = self.fetch('/v1/nodes' + '?node_key=' + new_key.key,
                         method='POST',
                         body=urlencode(post_data, doseq=True))
        self.assertEqual(200, res.code)

        res_data = json.loads(res.body)

        self.assertIsNotNone(res_data['name'])
        self.assertIsNotNone(res_data['id'])
        self.assertIsNotNone(res_data['display_name'])
        self.assertIsNotNone(res_data['tags'])
        self.assertEqual(len(res_data['tags']), 3)
        self.assertEqual(res_data['tags'], ['a', 'bb', 'ccc'])
        self.assertIsNotNone(res_data['is_online'])
        self.assertIsNotNone(res_data['client_ip'])

    def test_post_json(self):
        new_key = self._app.settings.get('node_manager').create_node_key()
        post_data = {}

        res = self.fetch('/v1/nodes' + '?node_key=' + new_key.key,
                         method='POST',
                         body=json.dumps(post_data),
                         headers={'Content-Type': 'application/json'})
        self.assertEqual(200, res.code)

        res_data = json.loads(res.body)

        self.assertIsNotNone(res_data['name'])
        self.assertIsNotNone(res_data['id'])
        self.assertIsNotNone(res_data['display_name'])
        self.assertIsNotNone(res_data['tags'])
        self.assertEqual(len(res_data['tags']), 0)
        self.assertIsNotNone(res_data['is_online'])
        self.assertIsNotNone(res_data['client_ip'])

    def test_post_json_full(self):
        new_key = self._app.settings.get('node_manager').create_node_key()
        post_data = {
            'tags': ['a', 'bb', 'ccc']
        }

        res = self.fetch('/v1/nodes' + '?node_key=' + new_key.key,
                         method='POST',
                         body=json.dumps(post_data),
                         headers={'Content-Type': 'application/json'})
        self.assertEqual(200, res.code)

        res_data = json.loads(res.body)

        self.assertIsNotNone(res_data['name'])
        self.assertIsNotNone(res_data['id'])
        self.assertIsNotNone(res_data['display_name'])
        self.assertIsNotNone(res_data['tags'])
        self.assertEqual(len(res_data['tags']), 3)
        self.assertEqual(res_data['tags'], ['a', 'bb', 'ccc'])
        self.assertIsNotNone(res_data['is_online'])
        self.assertIsNotNone(res_data['client_ip'])

    def test_post_no_node_key(self):
        new_key = self._app.settings.get('node_manager').create_node_key()
        post_data = {
            'tags': ['a', 'bb', 'ccc']
        }

        res = self.fetch('/v1/nodes',
                         method='POST',
                         body=json.dumps(post_data),
                         headers={'Content-Type': 'application/json'})
        # no NodeKey provided, should return 400
        self.assertEqual(400, res.code)

    def test_post_invalid_node_key(self):
        new_key = self._app.settings.get('node_manager').create_node_key()
        post_data = {
            'tags': ['a', 'bb', 'ccc']
        }

        # invalid node_key
        res = self.fetch('/v1/nodes?node_key=1',
                         method='POST',
                         body=json.dumps(post_data),
                         headers={'Content-Type': 'application/json'})
        # invalid NodeKey provided, should return 400
        self.assertEqual(400, res.code)


class NodeInstanceHandlerTest(NodeTest):
    def test_get(self):
        new_key = self._app.settings.get('node_manager').create_node_key()

        res = self.fetch('/v1/nodes' + '?node_key=' + new_key.key,
                         method='POST',
                         body='')
        self.assertEqual(200, res.code)

        res_data = json.loads(res.body)

        node_id = res_data['id']

        res = self.fetch('/v1/nodes/%s' % node_id)
        self.assertEqual(200, res.code)
        res_data = json.loads(res.body)
        self.assertIsNotNone(res_data['name'])
        self.assertIsNotNone(res_data['id'])
        self.assertIsNotNone(res_data['display_name'])
        self.assertIsNotNone(res_data['tags'])
        self.assertEqual(len(res_data['tags']), 0)
        self.assertIsNotNone(res_data['is_online'])
        self.assertIsNotNone(res_data['client_ip'])


class ObtainNodeSessionJobHandlerTest(NodeTest):
    def test_post(self):
        with session_scope() as session:
            user = self.get_user()
            project_manager = self.project_manager
            scheduler_manager = self.scheduler_manager
            node_manager = self._app.settings.get('node_manager')
            project_name = 'ObtainNodeSessionJobHandlerTest'
            exist_project = project_manager.get_project_by_name(
                session, user, project_name)
            if exist_project:
                project_manager.delete_project(user.id, exist_project.id)
                session.expunge(exist_project)

            project = project_manager.create_project(session,
                                                          user,
                                                          project_name)
            node_session = node_manager.create_node_session(session)
            spider = project_manager.create_spider(session, project, 'test')
            new_job = scheduler_manager.add_spider_task(session, spider)

            res = self.fetch('/v1/nodeSessions/%s/jobs:obtain' % node_session.id,
                             method='POST', body='')
            self.assertEqual(200, res.code)
            res_data = json.loads(res.body)
            self.assertIsNotNone(res_data['name'])

            # obtain one more
            # this time, there is no job available, should return 404
            res = self.fetch(
                '/v1/nodeSessions/%s/jobs:obtain' % node_session.id,
                method='POST', body='')
            self.assertEqual(404, res.code)

            project_manager.delete_project(user.id, project.id)


class CompleteNodeSessionJobHandlerTest(NodeTest):
    def test_post(self):
        with session_scope() as session:
            user = self.get_user()
            project_manager = self.project_manager
            scheduler_manager = self.scheduler_manager
            node_manager = self._app.settings.get('node_manager')
            project_name = 'CompoleteNodeSessionJobHandlerTest'
            exist_project = project_manager.get_project_by_name(
                session, user, project_name)
            if exist_project:
                project_manager.delete_project(user.id, exist_project.id)
                session.expunge(exist_project)

            project = project_manager.create_project(session,
                                                          user,
                                                          project_name)
            node_session = node_manager.create_node_session(session)
            spider = project_manager.create_spider(session, project, 'test')
            new_job = scheduler_manager.add_spider_task(session, spider)

            res = self.fetch('/v1/nodeSessions/%s/jobs:obtain' % node_session.id,
                             method='POST', body='')
            self.assertEqual(200, res.code)
            res_data = json.loads(res.body)
            self.assertIsNotNone(res_data['name'])
            job_id = res_data['id']

            complete_data = {'status': 'success'}
            complete_res = self.fetch('/v1/nodeSessions/%s/jobs/%s:complete' %
                                      (node_session.id, job_id),
                                      method='POST',
                                      body=urlencode(complete_data))
            self.assertEqual(200, complete_res.code)

            project_manager.delete_project(user.id, project.id)
