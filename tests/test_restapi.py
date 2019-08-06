from tornado.testing import AsyncHTTPTestCase
import json
from scrapydd.main import *
from six.moves.urllib_parse import urlencode, urlparse
from scrapydd.security import HmacAuthorize, generate_digest
from tornado.httpclient import HTTPRequest


def authenticated_request(*args, **kwargs):
    app_key = kwargs.pop("app_key")
    app_secret = kwargs.pop("app_secret")

    if len(args) > 0:
        url = args[0]
    elif "url" in kwargs:
        url = kwargs["url"]
    else:
        raise TypeError("Missing argument: 'url'")

    parsed_url = urlparse(url)

    path = parsed_url.path
    query = parsed_url.query

    body = kwargs.get("body", "")
    if isinstance(body, str):
        body = body.encode("utf-8")

    digest = generate_digest(app_secret, kwargs.get("method", "GET"), path, query, body)

    headers = kwargs.get("headers", {})
    headers["Authorization"] = "HMAC {} {}".format(app_key, digest)
    kwargs["headers"] = headers

    return HTTPRequest(*args, **kwargs)

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
        return make_app(scheduler_manager, node_manager, None)

    def fetch_request(self, request, **kwargs):
        self.http_client.fetch(request, self.stop, **kwargs)
        return self.wait()


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

        with session_scope() as session:
            existing_key = session.query(UserKey).filter_by(app_key=self.appkey).first()
            if not existing_key:
                userkey = UserKey(user_id=1, app_key=self.appkey, app_secret=self.appsecret)
                session.add(userkey)
                session.commit()


        return make_app(scheduler_manager, node_manager, authentication_providers=[HmacAuthorize()])


    def test_next_job(self):
        post_data = {'node_id': '1'}
        body = urlencode(post_data)
        path = '/api/v1/jobs/next'
        request = authenticated_request(url=self.get_url(path), method='POST', body=body, app_key=self.appkey,
                                        app_secret=self.appsecret)
        response = self.fetch_request(request)
        self.assertEqual(200, response.code)

    def test_next_job_wrong_app_key(self):
        post_data = {'node_id': '1'}
        body = urlencode(post_data)
        path = '/api/v1/jobs/next'
        request = authenticated_request(url=self.get_url(path), method='POST', body=body, app_key='somethingwrong',
                                        app_secret=self.appsecret)
        response = self.fetch_request(request)
        self.assertEqual(403, response.code)

    def test_next_job_wrong_app_secret(self):
        post_data = {'node_id': '1'}
        body = urlencode(post_data)
        path = '/api/v1/jobs/next'
        request = authenticated_request(url=self.get_url(path), method='POST', body=body, app_key=self.appkey,
                                        app_secret='somethingwrong')
        response = self.fetch_request(request)
        self.assertEqual(403, response.code)