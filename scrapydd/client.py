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
    def __init__(self, base_url, tags=None,
                 app_key=None, app_secret=None,
                 node_id=None):
        self._session = requests.Session()
        self._base_url = base_url
        self._node_id = node_id
        self._session_id = None
        self._tags = tags
        self._auth = None
        self._app_key = app_key
        if app_key and app_secret:
            self._auth = HmacAuth(app_key, app_secret)
            self._session.auth = self._auth

    def register_node(self, node_key):
        url = urljoin(self._base_url, '/v1/nodes')
        post_data = {
            'tags': self._tags
        }

        res = self._session.post(url,
                                 params={'node_key': node_key},
                                 json=post_data)
        res.raise_for_status()
        res_data = res.json()
        return res_data

    def login(self):
        url = urljoin(self._base_url, '/v1/nodeSessions')
        post_data = {
            'node_id': self._node_id
        }
        try:
            res = self._session.post(url, json=post_data)
            res.raise_for_status()
        except requests.exceptions.ConnectionError as ex:
            raise ConnectionError()
        except requests.exceptions.HTTPError as ex:
            if ex.response.status_code == 401:
                raise LoginFailedException()
            raise
        res_data = res.json()
        self._session_id = res_data['id']
        self._node_id = res_data['node_id']

    def heartbeat(self, running_job_ids=None):
        url = urljoin(self._base_url,
                      '/v1/nodeSessions/%s:heartbeat' % self._session_id)
        post_data = {
            'running_job_ids': running_job_ids or []
        }
        response = self._session.post(url=url,
                                      json=post_data)
        try:
            response.raise_for_status()
        except requests.HTTPError as ex:
            if ex.response.status_code == 401:
                raise NodeExpiredException()
            raise
        res_data = response.json()
        return res_data

    def get_next_job(self):
        url = urljoin(self._base_url,
                      '/v1/nodeSessions/%s/jobs:obtain' % self._session_id)
        response = self._session.post(url, data={'node_id': self._node_id})
        try:
            response.raise_for_status()
        except requests.HTTPError as ex:
            if ex.response.status_code == 404:
                raise NoJobAvailable()
            if ex.response.status_code == 401:
                raise NodeExpiredException()
            raise
        response_data = response.json()
        task = SpiderTask()
        task.id = response_data['id']
        task.figure = json.loads(response_data['figure'])
        return task

    def get_job_egg(self, job_id):
        egg_request_url = urljoin(self._base_url,
                                  '/v1/nodeSessions/%s/jobs/%s/egg' % (
                                  self._session_id, job_id))
        response = self._session.get(egg_request_url)
        response.raise_for_status()
        return BytesIO(response.content)

    def complete_job(self, job_id, status='success', items_file=None,
                     logs_file=None):
        url = urljoin(self._base_url,
                      '/v1/nodeSessions/%s/jobs/%s:complete' % (
                      self._session_id, job_id))
        post_data = {
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
            post_files['logs'] = f_logs
        try:
            response = self._session.post(url, data=post_data,
                                          files=post_files)
            response.raise_for_status()
        finally:
            if f_items:
                f_items.close()
            if f_logs:
                f_logs.close()

    def close(self):
        pass


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
    def __init__(self):
        # super(LoginFailedException, self).__init__(message=)
        self.message = 'Login failed'

    def __str__(self):
        return 'LoginFailedException: %s' % self.message


class NoJobAvailable(Exception):
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


class ClientCredentialInterceptor(
    generic_client_interceptor._GenericClientInterceptor):
    def __init__(self, client):
        super(ClientCredentialInterceptor, self).__init__(self.call)
        self.client = client

    def call(self, client_call_details, request_iterator,
             request_streaming,
             response_streaming):
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)

        if client_call_details.method != '/NodeService/Login' and self.client.node_id:
            metadata.append((
                'x-node-id', str(self.client.node_id),
            ))
        client_call_details = _ClientCallDetails(
            client_call_details.method, client_call_details.timeout, metadata,
            client_call_details.credentials)
        return client_call_details, request_iterator, None


class NodeGrpcClient:
    def __init__(self, base_url, server_cert=None, ca_cert=None, node_id=None,
                 app_key=None, app_secret=None, tags=None):
        urlparts = urlparse(base_url)
        if urlparts.scheme == 'grpcs':
            use_tls = True
        else:
            use_tls = False
        self._app_key = app_key
        self._app_secret = app_secret
        self._tags = tags
        self._node_id = node_id
        host = urlparts.hostname
        port = urlparts.port
        logger.debug('node_id: %s', node_id)
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

        self.interceptor = ClientCredentialInterceptor(self)
        channel = grpc.intercept_channel(channel, self.interceptor)
        self._channel = channel
        self._node_stub = service_pb2_grpc.NodeServiceStub(channel)

    @property
    def node_id(self):
        return self._node_id

    def connect(self):
        pass

    def login(self):
        request = service_pb2.CreateNodeSessionRequest()
        if self._node_id:
            request.node_session.node_id = self._node_id
        try:
            response = self._node_stub.CreateNodeSession(request)
        except grpc.RpcError as ex:
            logger.debug(ex)
            if ex.code() == grpc.StatusCode.UNAUTHENTICATED:
                raise LoginFailedException()
            raise

        node_id = response.node_id
        session_id = response.id
        logger.info('node_id %s logged in.', node_id)

        self._node_id = node_id
        self._session_id = session_id

    def heartbeat(self, running_job_ids: None):
        request = service_pb2.HeartbeatNodeSessionRequest()
        request.name = 'nodeSessions/%s' % self._session_id
        if running_job_ids:
            for running_job_id in running_job_ids:
                request.running_job_ids.append(running_job_id)
        try:
            response = self._node_stub.HeartbeatNodeSession(request)
        except grpc.RpcError as ex:
            if ex.code() in (grpc.StatusCode.UNAUTHENTICATED,
                             grpc.StatusCode.NOT_FOUND):
                self._node_id = None
                raise NodeExpiredException()
            raise

        res_data = {
            'kill_job_ids': response.kill_job_ids,
            'new_job_available': response.new_job_available,
        }
        return res_data

    def get_next_job(self):
        request = service_pb2.ObtainNodeSessionJobRequest()
        request.name = 'nodeSessions/%s/jobs' % self._session_id
        try:
            response = self._node_stub.ObtainNodeSessionJob(request)
        except grpc.RpcError as ex:
            if ex.code() == grpc.StatusCode.NOT_FOUND:
                raise NoJobAvailable()
            raise

        task = SpiderTask()
        task.id = response.id
        task.figure = json.loads(response.figure)
        return task

    def get_job_egg(self, job_id):
        request = service_pb2.GetNodeSessionJobEggRequest()
        request.name = 'nodeSessions/%s/jobs/%s/egg' % (
        self._session_id, job_id)
        buffer = BytesIO()
        for chunk in self._node_stub.GetNodeSessionJobEgg(request):
            buffer.write(chunk.data)
        buffer.seek(0)
        logger.debug('download job egg complete.')
        return buffer

    def complete_job(self, job_id, status='success', items_file=None,
                     logs_file=None):
        request = service_pb2.CompleteNodeSessionJobRequest()
        request.name = 'nodeSessions/%s/jobs/%s' % (self._session_id, job_id)
        request.status = status
        if items_file:
            with open(items_file, 'rb') as f:
                request.items = f.read()

        if logs_file:
            with open(logs_file, 'rb') as f:
                request.logs = f.read()

        response = self._node_stub.CompleteNodeSessionJob(request)
        return response

    def register_node(self, node_key):
        request = service_pb2.CreateNodeRequest()
        request.node_key = node_key
        for tag in self._tags or []:
            request.node.tags.append(tag)

        res = self._node_stub.CreateNode(request)
        ret = {
            'id': res.id,
            'name': res.name,
            'display_name': res.display_name,
            'tags': res.tags,
            'is_online': res.is_online,
            'client_ip': res.client_ip
        }
        return ret

    def close(self):
        self._node_stub = None
        if hasattr(self._channel, '_channel'):
            self._channel._channel = None
        self._channel = None

    def __del__(self):
        self._channel = None


def get_client(config, app_key=None, app_secret=None, node_id=None):
    server_url = config.get('server', 'http://localhost:6800')
    server_url_parts = urlparse(server_url)
    tags = config.get('tags', '')
    tags = [x for x in tags.split(',') if x]
    if not server_url_parts.scheme:
        client = NodeRestClient('http://%s:6800' % server_url,
                                tags=tags, app_key=app_key,
                                app_secret=app_secret, node_id=node_id)
        return client
    if server_url_parts.scheme in ('http', 'https'):
        client = NodeRestClient(server_url, tags=tags, app_key=app_key,
                                app_secret=app_secret, node_id=node_id)
        return client

    if server_url_parts.scheme in ('grpc', 'grpcs'):
        client = NodeGrpcClient(server_url, tags=tags, app_key=app_key,
                                app_secret=app_secret, node_id=node_id)
        return client

    raise Exception('Unsupported scheme: %s' % server_url_parts.scheme)
