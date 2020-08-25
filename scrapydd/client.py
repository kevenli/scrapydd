import collections

from urllib.parse import urljoin, urlparse
from io import BytesIO
import hashlib
import hmac
import json
import logging
from urllib.parse import parse_qs, quote

import requests
import grpc

from .grpcservice import service_pb2
from .grpcservice import service_pb2_grpc
from .grpcservice import generic_client_interceptor
from .utils import str2bool, ensure_binary

logger = logging.getLogger(__name__)


class SpiderTask:
    """
    Spider task description data fetched from server.
    """
    id = None
    spider_id = None
    project_name = None
    spider_name = None
    project_version = None
    spider_parameters = None
    extra_requirements = None
    settings = None
    figure = None
    package = None


class HearbeatResponse:
    def __init__(self, kill_jobs=None, has_new_job=None):
        self.kill_jobs = kill_jobs
        self.has_new_job = has_new_job

    @property
    def newJobAvailable(self):
        return self.has_new_job

    def __str__(self):
        return f'HearbeatResponse {{newJobAvailable: {self.newJobAvailable}}}'


class InsecureHeaderAuth(requests.auth.AuthBase):
    def __init__(self, node_id):
        self._node_id = str(node_id)

    def __call__(self, r):
        r.headers['X-Dd-Nodeid'] = self._node_id
        return r


def generate_digest(secret, method, path, query, body):
    parsed_query = parse_qs(query, keep_blank_values=True)

    canonical_query = []

    for key in sorted(parsed_query.keys()):
        for value in sorted(parsed_query[key]):
            canonical_query.append("=".join((key, quote(value))))
    body = ensure_binary(body)
    return hmac.new(
        secret.encode("utf-8"),
        "\n".join((method, path, "&".join(canonical_query), "")).encode(
            "utf-8") +
        body,
        hashlib.sha256).hexdigest()


class HmacAuth(requests.auth.AuthBase):
    def __init__(self, key, secret_key):
        self._key = key
        self._secret_key = secret_key

    def __call__(self, request):
        parsed_url = urlparse(request.url)
        path = parsed_url.path
        query = parsed_url.query
        method = request.method
        body = request.body or b''
        digest = generate_digest(self._secret_key, method, path, query,
                                 body)
        authorization_header = '%s %s %s' % ('HMAC', self._key, digest)
        request.headers['Authorization'] = authorization_header
        return request


class NodeRestClient:
    def __init__(self, base_url, auth=None):
        self._session = requests.Session()
        self._auth = auth
        self._session.auth = auth
        self._base_url = base_url
        self._node_id = None
        self._session_id = None

    def register_node(self, node_key):
        url = urljoin(self._base_url, '/nodes/register')
        post_data = {'node_key': node_key}
        res = self._session.post(url, data=post_data)
        res.raise_for_status()
        res_data = res.json()
        return res_data['id']

    def login(self):
        url = urljoin(self._base_url, '/api/nodeSessions')
        res = self._session.post(url)
        res.raise_for_status()
        res_data = res.json()
        self._session_id = res_data['id']
        self._node_id = res_data['node']['id']
        if self._auth is None:
            self._auth = InsecureHeaderAuth(self._node_id)
            self._session.auth = self._auth

    def heartbeat(self, running_job_ids=None):
        url = urljoin(self._base_url,
                      '/api/nodeSessions/%s:heartbeat' % self._session_id)
        running_tasks = ''
        if running_job_ids:
            running_tasks = ','.join(running_job_ids)
        response = self._session.post(url=url,
                                      data='',
                                      headers={
                                          'X-DD-RunningJobs': running_tasks})
        response.raise_for_status()
        new_job = str2bool(response.headers.get('X-DD-New-Task', 'false'))
        heartbeat_res = HearbeatResponse(has_new_job=new_job)
        return heartbeat_res

    def get_next_job(self):
        url = urljoin(self._base_url, '/executing/next_task')
        response = self._session.post(url, data={'node_id': self._node_id})
        response.raise_for_status()
        response_data = response.json()
        task = SpiderTask()
        task.id = response_data['data']['task']['task_id']
        task.spider_id = response_data['data']['task']['spider_id']
        task.project_name = response_data['data']['task']['project_name']
        task.project_version = response_data['data']['task']['version']
        task.spider_name = response_data['data']['task']['spider_name']
        if 'extra_requirements' in response_data['data']['task'] and \
                response_data['data']['task']['extra_requirements']:
            task.extra_requirements = response_data['data']['task'][
                'extra_requirements']
        if 'spider_parameters' in response_data['data']['task']:
            task.spider_parameters = response_data['data']['task'][
                'spider_parameters']
        else:
            task.spider_parameters = {}
        task.settings = response_data['data']
        if 'figure' in response_data['data']['task']:
            task.figure = response_data['data']['task']['figure']
        return task

    def get_job_egg(self, job_id):
        egg_request_url = urljoin(self._base_url, '/jobs/%s/egg' % job_id)
        response = self._session.get(egg_request_url)
        response.raise_for_status()
        return BytesIO(response.content)

    def complete_job(self, job_id, status='success', items_file=None,
                     logs_file=None):
        url = urljoin(self._base_url, '/executing/complete')
        post_data = {
            'task_id': job_id,
            'status': status,
        }
        post_files = {}
        f_items = None
        f_logs = None
        if items_file:
            f_items = open(items_file, 'rb')
            post_files['items'] = f_items
        if logs_file:
            f_logs = open(logs_file, 'rb')
            post_files['log'] = f_logs
        try:
            response = self._session.post(url, data=post_data,
                                          files=post_files)
            response.raise_for_status()
        finally:
            if f_items:
                f_items.close()
            if f_logs:
                f_logs.close()


class UsernamePasswordCallCredentials(grpc.AuthMetadataPlugin):
    def __init__(self, username):
        self._username = username

    def __call__(self, context, callback):
        metadata = (('x-custom-auth-ticket', self._username),)
        callback(metadata, None)


class NodeCallCredentials(grpc.AuthMetadataPlugin):
    def __init__(self, node_id):
        self._node_id = node_id

    def __call__(self, context, callback):
        metadata = (('x-node-id', str(self._node_id)),)
        logger.debug(metadata)
        callback(metadata, None)


class NodeExpiredException(Exception):
    pass


class LoginFailedException(Exception):
    pass


class _ClientCallDetails(
    collections.namedtuple(
        '_ClientCallDetails',
        ('method', 'timeout', 'metadata', 'credentials')),
    grpc.ClientCallDetails):
    pass


def header_adder_interceptor(header, value):
    def intercept_call(client_call_details, request_iterator,
                       request_streaming,
                       response_streaming):
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)
        metadata.append((
            header,
            value,
        ))
        client_call_details = _ClientCallDetails(
            client_call_details.method, client_call_details.timeout, metadata,
            client_call_details.credentials)
        return client_call_details, request_iterator, None

    return generic_client_interceptor.create(intercept_call)


class NodeGrpcClient:
    def __init__(self, base_url, server_cert=None, ca_cert=None, node_id=None):
        urlparts = urlparse(base_url)
        if urlparts.scheme == 'grpcs':
            use_tls = True
        else:
            use_tls = False
        host = urlparts.hostname
        port = urlparts.port
        logger.debug('node_id: %s', node_id)
        # logger.debug('token: %s', token)
        # with open('server.crt', 'rb') as f:
        #     server_cert = f.read()
        max_message_length = 100 * 1024 * 1024
        channel_opt = [('grpc.max_send_message_length', max_message_length),
                       ('grpc.max_receive_message_length', max_message_length)]

        if use_tls:
            credentials = grpc.ssl_channel_credentials(
                root_certificates=server_cert)
            call_creds = grpc.metadata_call_credentials(
                NodeCallCredentials(node_id))
            channel_creds = grpc.composite_channel_credentials(credentials,
                                                               call_creds)

            channel = grpc.secure_channel('{}:{}'.format(host, port),
                                          channel_creds, options=channel_opt)
        else:
            channel = grpc.insecure_channel('{}:{}'.format(host, port),
                                            options=channel_opt)

        self._channel = channel
        self._node_stub = service_pb2_grpc.NodeServiceStub(channel)

    def connect(self):
        pass

    def login(self):
        request = service_pb2.LoginRequest()
        try:
            response = self._node_stub.Login(request)
        except grpc.RpcError as ex:
            if ex.code() == grpc.StatusCode.UNAUTHENTICATED:
                raise LoginFailedException()
            raise
        if response.nodeId == 0:
            raise LoginFailedException()

        node_id = response.nodeId
        logger.info('node_id %s logged in.', node_id)

        self._node_id = node_id
        interceptor = header_adder_interceptor('x-node-id', str(node_id))
        self._channel = grpc.intercept_channel(self._channel, interceptor)
        self._node_stub = service_pb2_grpc.NodeServiceStub(self._channel)

    def heartbeat(self, running_job_ids: None):
        request = service_pb2.HeartbeatRequest()
        if running_job_ids:
            for running_job_id in running_job_ids:
                request.runningJobs.append(running_job_id)
        response = self._node_stub.Heartbeat(request)
        if response.nodeExpired:
            raise NodeExpiredException()
        logger.debug(f'HeartbeatResponse, nodeExpired:{response.nodeExpired}, '
                     f'killJobs:{response.killJobs}, newJobAvailable:{response.newJobAvailable}')
        return response

    def get_next_job(self):
        request = service_pb2.GetNextJobRequest()
        response = self._node_stub.GetNextJob(request)
        if not response.jobId:
            return None

        task = SpiderTask()
        task.id = response.jobId
        task.figure = json.loads(response.figure)
        task.package = BytesIO(response.package)
        return task

    def complete_job(self, job_id, status='success', items_file=None,
                     logs_file=None):
        request = service_pb2.CompleteJobRequest()
        request.jobId = job_id
        request.status = status
        if items_file:
            with open(items_file, 'rb') as f:
                request.items = f.read()

        if logs_file:
            with open(logs_file, 'rb') as f:
                request.logs = f.read()

        response = self._node_stub.CompleteJob(request)
        return response

    def close(self):
        self._channel = None

    def __del__(self):
        self._channel = None


def get_client(config, app_key=None, app_secret=None):
    server_url = config.get('server', 'http://localhost:6800')
    server_url_parts = urlparse(server_url)
    if not server_url_parts.scheme:
        auth = None
        if app_key and app_secret:
            auth = HmacAuth(app_key, app_secret)
        client = NodeRestClient('http://%s:6800' % server_url,
                                auth=auth)
        return client
    if server_url_parts.scheme in ('http', 'https'):
        auth = None
        if app_key and app_secret:
            auth = HmacAuth(app_key, app_secret)
        client = NodeRestClient(server_url, auth=auth)
        return client

    if server_url_parts.scheme in ('grpc', 'grpcs'):
        client = NodeGrpcClient(server_url)
        return client

    raise Exception('Unsupported scheme: %s' % server_url_parts.scheme)
