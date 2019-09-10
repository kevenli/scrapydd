from six.moves.urllib_parse import urlencode, urlparse
from scrapydd.security import authenticated_request
from .base import AppTest, SecureAppTest
from scrapydd.models import session_scope, SpiderExecutionQueue, Node
import json
import datetime


class ScheduleTest(AppTest):
    def test_schedule(self):
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
        with session_scope() as session:
            session.query(SpiderExecutionQueue).delete()
            session.commit()

        post_params = {
            'project': 'test_project',
            'spider': 'success_spider'
        }
        headers = {}
        headers = self.populate_basic_authorization_header(headers)
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


# class GetNextJobTest(AppTest):
#     def test_next_job(self):
#         post_data = {'node_id': '1'}
#         body = urlencode(post_data)
#         response = self.fetch('/api/v1/jobs/next', method="POST", body=body)
#         self.assertEqual(200, response.code)
#

# class GetNextJobSecureTest(AppTest):
#     def test_next_job(self):
#         post_data = {'node_id': self.node_id}
#         body = urlencode(post_data)
#         path = '/api/v1/jobs/next'
#         request = authenticated_request(url=self.get_url(path), method='POST', body=body, app_key=self.appkey,
#                                         app_secret=self.appsecret)
#         response = self.fetch_request(request)
#         self.assertEqual(200, response.code)
#
#     def test_next_job_wrong_app_key(self):
#         post_data = {'node_id': self.node_id}
#         body = urlencode(post_data)
#         path = '/api/v1/jobs/next'
#         request = authenticated_request(url=self.get_url(path), method='POST', body=body, app_key='somethingwrong',
#                                         app_secret=self.appsecret)
#         response = self.fetch_request(request)
#         self.assertEqual(403, response.code)
#
#     def test_next_job_wrong_app_secret(self):
#         post_data = {'node_id': self.node_id}
#         body = urlencode(post_data)
#         path = '/api/v1/jobs/next'
#         request = authenticated_request(url=self.get_url(path), method='POST', body=body, app_key=self.appkey,
#                                         app_secret='somethingwrong')
#         response = self.fetch_request(request)
#         self.assertEqual(403, response.code)