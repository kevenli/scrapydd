"""
Handler for agent node communicating
"""
# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
import datetime
import os
import json
import logging
import hmac
import tornado
from io import BytesIO
import shutil
from tornado.web import authenticated, HTTPError
from six import ensure_str, binary_type
from .base import RestBaseHandler
from ..models import session_scope, Session, Spider, Project, Node
from ..models import SpiderExecutionQueue, SpiderSettings, NodeKey
from ..stream import PostDataStreamer
from ..exceptions import NodeExpired
from ..security import generate_digest
from ..nodes import NodeManager, AnonymousNodeDisabled
from .. import nodes

LOGGER = logging.getLogger(__name__)


class InvalidHmacCredentialError(tornado.web.HTTPError):
    def __init__(self):
        super(InvalidHmacCredentialError, self).__init__(401,
                                                    'Invalid Hamc Credential')


class NodeHmacAuthenticationProvider:
    def get_user(self, handler):
        node_key = self.validate_request(handler)
        if node_key:
            return node_key.used_node_id

        return None

    def validate_request(self, handler):
        authorization = handler.request.headers.get("Authorization", "") \
            .split(" ")
        if len(authorization) != 3:
            LOGGER.debug("No Authorization header %s", authorization)
            return None

        algorithm, key, provided_digest = authorization
        if algorithm != "HMAC":
            LOGGER.debug("Not HMAC Authorization %s", algorithm)
            return None

        with session_scope() as session:
            node_key = session.query(NodeKey).filter_by(key=key).first()

            if node_key is None:
                LOGGER.info("Invalid HMAC key %s", key)
                raise InvalidHmacCredentialError()
            secret = node_key.secret_key
            body = handler.request.body if isinstance(handler.request.body,
                                                      binary_type) else b''
            expected_digest = generate_digest(
                secret, handler.request.method, handler.request.path,
                handler.request.query,
                body)

            if not hmac.compare_digest(expected_digest, provided_digest):
                LOGGER.info("Invalid HMAC digest %s", provided_digest)
                raise InvalidHmacCredentialError()
            return node_key


class NodeBaseHandler(RestBaseHandler):
    authentication_providers = [NodeHmacAuthenticationProvider()]

    def get_current_user(self):
        # if enable_node_registration is not enabled, node do not
        # need register to work.
        # get node_id from request
        if not self.settings.get('enable_node_registration', False):
            str_node_id = self.request.headers.get('X-Dd-Nodeid')
            if str_node_id:
                return int(str_node_id)

        for authentication_provider in self.authentication_providers:
            node_id = authentication_provider.get_user(self)
            return node_id

    @property
    def node_manager(self) -> NodeManager:
        return self.settings.get('node_manager')


class RegisterNodeHandler(NodeBaseHandler):
    # pylint: disable=arguments-differ
    def post(self):
        provider = NodeHmacAuthenticationProvider()
        node_key = provider.validate_request(self)
        if not node_key:
            return self.set_status(403, 'invalid key')

        with session_scope() as session:
            tags = self.get_argument('tags', '').strip()
            tags = None if tags == '' else tags
            remote_ip = self.request.headers.get('X-Real-IP',
                                                 self.request.remote_ip)
            node = self.node_manager.create_node(remote_ip, tags=tags,
                                            key_id=node_key.id)
            node_key.used_node_id = node.id
            session.add(node_key)
            session.commit()
            return self.send_json({'id': node.id})


class NodesHandler(NodeBaseHandler):
    # pylint: disable=arguments-differ
    def initialize(self, node_manager):
        super(NodesHandler, self).initialize()

    # pylint: disable=arguments-differ
    def post(self):
        enable_node_registration = self.settings.get('enable_node_registration'
                                                     , False)
        node_id = NodeHmacAuthenticationProvider().get_user(self)

        # if node_registerion is enabled, node should be assigned
        # node_id before use by
        # invoke /nodes/register
        LOGGER.debug('enable_node_registration : %s', enable_node_registration)
        if enable_node_registration and node_id is None:
            return self.set_status(403, 'node registration is enabled, '
                                        'no key provided.')

        tags = self.get_argument('tags', '').strip()
        tags = None if tags == '' else tags
        remote_ip = self.request.headers.get('X-Real-IP',
                                             self.request.remote_ip)
        try:
            node = self.node_manager.node_online(self.session, node_id, remote_ip,
                                             tags)
            return self.write(json.dumps({'id': node.id}))
        except AnonymousNodeDisabled:
            return self.set_status(403, 'AnonymousNodeDisabled')


class ExecuteNextHandler(NodeBaseHandler):
    scheduler_manager = None

    # pylint: disable=arguments-differ
    def initialize(self, scheduler_manager=None):
        self.scheduler_manager = scheduler_manager

    # pylint: disable=arguments-differ
    @authenticated
    def post(self):
        with session_scope() as session:
            node_id = int(self.get_argument('node_id'))
            next_task = self.scheduler_manager.get_next_task(node_id)

            response_data = {'data': None}

            if not next_task:
                return self.write(json.dumps(response_data))

            spider = session.query(Spider)\
                .filter_by(id=next_task.spider_id)\
                .first()
            if not spider:
                LOGGER.error('Task %s has not spider, deleting.',
                             next_task.id)
                session.query(SpiderExecutionQueue)\
                    .filter_by(id=next_task.id)\
                    .delete()
                return self.write({'data': None})

            project = session.query(Project)\
                .filter_by(id=spider.project_id)\
                .first()
            if not project:
                LOGGER.error('Task %s has not project, deleting.',
                             next_task.id)
                session.query(SpiderExecutionQueue)\
                    .filter_by(id=next_task.id)\
                    .delete()
                return self.write({'data': None})

            extra_requirements_setting = session.query(SpiderSettings) \
                .filter_by(spider_id=spider.id,
                           setting_key='extra_requirements').first()


            if extra_requirements_setting and extra_requirements_setting.value:
                extra_requirements = [x for x
                                      in extra_requirements_setting.value.split(';')
                                      if x]
            else:
                extra_requirements = []
            task = {
                'task_id': next_task.id,
                'spider_id': next_task.spider_id,
                'spider_name': next_task.spider_name,
                'project_name': next_task.project_name,
                'version': project.version,
                'extra_requirements': extra_requirements,
                'spider_parameters': {parameter.parameter_key: parameter.value
                                      for parameter in spider.parameters},
                'figure': self.project_manager.get_job_figure(session, next_task),
            }

            figure = self.project_manager.get_job_figure(session, next_task)
            if figure:
                task['figure'] = figure.to_dict()

            LOGGER.debug('job_settings: %s', task)
            LOGGER.debug('next_task.settings: %s', next_task.settings)
            job_specific_settings = json.loads(next_task.settings) \
                if next_task.settings else {}
            if 'spider_parameters' in job_specific_settings:
                task['spider_parameters'] \
                    .update(**job_specific_settings['spider_parameters'])
            LOGGER.debug('job_settings: %s', task)
            response_data['data'] = {'task': task}
            return self.write(json.dumps(response_data))


MB = 1024 * 1024
GB = 1024 * MB
TB = 1024 * GB
MAX_STREAMED_SIZE = 1 * GB


@tornado.web.stream_request_body
class ExecuteCompleteHandler(NodeBaseHandler):
    webhook_daemon = None
    scheduler_manager = None
    ps = None

    # pylint: disable=arguments-differ
    def initialize(self, webhook_daemon=None, scheduler_manager=None):
        """

        @type webhook_daemon : WebhookDaemon

        @type scheduler_manager: SchedulerManager
        :return:
        """
        super(ExecuteCompleteHandler, self).initialize()
        self.webhook_daemon = webhook_daemon
        self.scheduler_manager = scheduler_manager

    def prepare(self):
        # set the max size limiation here
        self.request.connection.set_max_body_size(MAX_STREAMED_SIZE)
        try:
            total = int(self.request.headers.get("Content-Length", "0"))
        except TypeError:
            total = 0
        self.ps = PostDataStreamer(total)

    def _complete_operation(self):
        fields = self.ps.get_values(['task_id', 'status'])
        LOGGER.debug(self.ps.get_nonfile_names())
        node_id = self.request.headers.get('X-Dd-Nodeid')
        task_id = ensure_str(fields['task_id'])
        status = ensure_str(fields['status'])
        if status == 'success':
            status_int = 2
        elif status == 'fail':
            status_int = 3
        else:
            self.set_status(401, 'Invalid argument: status.')
            return

        session = Session()
        query = session.query(SpiderExecutionQueue) \
            .filter(SpiderExecutionQueue.id == task_id,
                    SpiderExecutionQueue.status.in_([1, 5]))
        # be compatible with old agent version
        if node_id:
            query = query.filter(SpiderExecutionQueue.node_id == node_id)
        else:
            LOGGER.warning('Agent has not specified node id in '
                           'complete request, client address: %s.',
                           self.request.remote_ip)
        job = query.first()

        if job is None:
            self.set_status(404, 'Job not found.')
            session.close()
            return
        items_file = None
        log_stream = BytesIO()
        try:
            spider_log_folder = os.path.join('logs', job.project_name,
                                             job.spider_name)
            if not os.path.exists(spider_log_folder):
                os.makedirs(spider_log_folder)

            log_part = self.ps.get_parts_by_name('log')[0]
            if log_part:
                with open(log_part['tmpfile'].name, 'rb') as f:
                    shutil.copyfileobj(f, log_stream)

        except Exception as ex:
            LOGGER.error('Error when writing task log file, %s', ex)

        items_parts = self.ps.get_parts_by_name('items')
        items_stream = BytesIO()
        if items_parts:
            try:
                part = items_parts[0]
                tmpfile = part['tmpfile'].name
                LOGGER.debug('tmpfile size: %d', os.path.getsize(tmpfile))
                items_file_path = os.path.join('items', job.project_name,
                                               job.spider_name)
                if not os.path.exists(items_file_path):
                    os.makedirs(items_file_path)
                items_file = os.path.join(items_file_path,
                                          '%s.jl' % job.id)
                with open(tmpfile, 'rb') as f:
                    shutil.copyfileobj(f, items_stream)
            except Exception as ex:
                LOGGER.error('Error when writing items file, %s', ex)

        job.status = status_int
        job.update_time = datetime.datetime.now()
        historical_job = self.scheduler_manager.job_finished(job,
                                                             log_stream,
                                                             items_stream)
        #
        # if items_file:
        #     self.webhook_daemon.on_spider_complete(historical_job,
        #                                            items_file)
        session.close()
        LOGGER.info('Job %s completed.', task_id)
        response_data = {'status': 'ok'}
        self.write(json.dumps(response_data))

    # pylint: disable=arguments-differ
    @authenticated
    def post(self):
        try:
            self.ps.finish_receive()
            return self._complete_operation()
        finally:
            # Don't forget to release temporary files.
            self.ps.release_parts()

    def data_received(self, chunk):
        self.ps.receive(chunk)


class NodeHeartbeatHandler(NodeBaseHandler):
    # pylint: disable=arguments-differ
    def initialize(self, node_manager, scheduler_manager):
        super(NodeHeartbeatHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    # pylint: disable=arguments-differ
    @authenticated
    def post(self, node_id):
        node_id = int(self.current_user)
        has_task = self.scheduler_manager.has_task(node_id)
        self.set_header('X-DD-New-Task', has_task)
        try:
            self.node_manager.heartbeat(node_id)
        except NodeExpired:
            response_data = {'status': 'error', 'errmsg': 'Node expired'}
            self.set_status(400, 'Node expired')
            return self.write(json.dumps(response_data))
        running_jobs = self.request.headers.get('X-DD-RunningJobs', '')
        running_job_ids = [x for x in running_jobs.split(',') if x]
        if running_jobs:
            killing_jobs = list(self.scheduler_manager.jobs_running(node_id,
                                                               running_job_ids))
            if killing_jobs:
                LOGGER.info('killing %s', killing_jobs)
                self.set_header('X-DD-KillJobs',
                                json.dumps(list(killing_jobs)))
        response_data = {'status': 'ok'}
        return self.write(json.dumps(response_data))


class JobStartHandler(NodeBaseHandler):
    # pylint: disable=arguments-differ
    def initialize(self, scheduler_manager):
        super(JobStartHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    # pylint: disable=arguments-differ
    @authenticated
    def post(self, jobid):
        pid = self.get_argument('pid')
        pid = int(pid) if pid else None
        self.scheduler_manager.job_start(jobid, pid)


class JobEggHandler(NodeBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def get(self, jobid):
        job = self.session.query(SpiderExecutionQueue) \
            .filter_by(id=jobid) \
            .first()
        if not job:
            raise tornado.web.HTTPError(404)
        f_egg = self.project_manager.get_job_egg(self.session, job)
        self.write(f_egg.read())


ANONYMOUS_NODE = Node(id=None)


class NodeApiBaseHandler(NodeBaseHandler):
    """
    This provides the base handler class for new rest apis.
    These api handlers do not require the custom header x-dd-nodeid
    as a context in request processing. The necessary context infomation
    are all in the url segments.
    These apis are all follow the google api guide and following
    prefix "/v1" .
    In these apis, if `eanble_authentication` is turned on, authentication
    info should be provided on each request, including the login action.
    For now, the auth mechanism is through a HMAC validation, this may
    change in the future, but along with the authentication framework,
    handlers should not beware of the change. Handler use `current_user`
    is enough.
    """
    authentication_providers = [NodeHmacAuthenticationProvider()]
    json_data = None

    def get_current_user(self):
        for authentication_provider in self.authentication_providers:
            node_id = authentication_provider.get_user(self)
            if node_id:
                if isinstance(node_id, int):
                    return self.session.query(Node).get(node_id)
                return node_id

        if not self.settings.get('enable_authentication', False):
            return ANONYMOUS_NODE

    def get_node_session(self, node_session_id):
        """
        NodeSession instance handler should call this first to validate
        node_session and current context matching first.
        :param node_session_id: node_session_id in url
        :return:
            NodeSession object if valid.
            Fire 404 Error if context not match.
        """
        node_session = self.node_manager.get_node_session(self.session,
                                                          node_session_id,
                                                          self.current_user)

        if node_session is None:
            raise tornado.web.HTTPError(404, 'NodeSession not found.')

        return node_session

    def request_is_json(self):
        content_type_header = self.request.headers.get('content-type', 'form')
        return content_type_header.lower() == 'application/json'

    def get_argument(self, name: str, default=None, strip: bool = True):
        if self.request_is_json():
            self.json_data = json.loads(self.request.body)
            return self.json_data.get(name, default)

        return super(NodeApiBaseHandler, self).get_argument(
                name, default, strip)

    def get_arguments(self, name, strip: bool = True):
        if self.request_is_json():
            self.json_data = json.loads(self.request.body)
            return self.json_data.get(name)

        return super(NodeApiBaseHandler, self)\
                .get_arguments(name, strip)

    def get_body_arguments(self, name: str, strip: bool = True):
        if self.request_is_json():
            self.json_data = json.loads(self.request.body)
            return self.json_data.get(name, [])
        return super(NodeApiBaseHandler, self)\
                .get_body_arguments(name, strip)


def node_authenticated(method):
    def wrapper(self, *args, **kwargs):
        if not self.current_user:
            raise tornado.web.HTTPError(401)
        return method(self, *args, **kwargs)

    return wrapper


class NodeSessionListHandler(NodeApiBaseHandler):
    @node_authenticated
    def post(self):
        node = self.current_user
        node_id = int(self.get_argument('node_id') or 0)
        if node.id and node.id != node_id:
            raise HTTPError(400, 'invalid node_id')
        session = self.session
        remote_ip = self.request.headers.get('X-Real-IP',
                                             self.request.remote_ip)
        node_manager = self.node_manager
        try:
            node_session = node_manager.create_node_session(
                session,
                node=node,
                client_ip=remote_ip)
            if node_session is None:
                return self.set_status(401, 'Invalid token.')
        except AnonymousNodeDisabled:
            return self.set_status(401, 'Anonymous node not allowed.')
        except nodes.NodeNotFound:
            return self.set_status(401, 'Node not found.')
        except nodes.LivingNodeSessionExistException:
            return self.set_status(409, 'There are some NodeSession living'
                                        ' already relates to the target Node.')

        res_data = {
            'name': 'nodeSessions/%s' % node_session.id,
            'id': node_session.id,
            'node_id': node_session.node.id
        }
        return self.send_json(res_data)


class NodeSessionInstanceHeartbeatHandler(NodeApiBaseHandler):
    @node_authenticated
    def post(self, session_id):
        node_session = self.get_node_session(session_id)
        node_manager = self.node_manager
        try:
            node_session = node_manager.node_session_heartbeat(self.session,
                                                           session_id)
        except nodes.NodeExpired:
            return self.set_status(401, 'Node Expired')
        node = node_session.node
        node_id = node.id
        has_task = node_manager.node_has_task(self.session, node_id)

        running_job_ids = self.get_body_arguments('running_job_ids')
        # if isinstance(running_job_ids, str):
        #     running_job_ids = [x for x in running_job_ids.split(',') if x]

        killing_jobs = list(self.node_manager.jobs_running(self.session,
                                                           node_id,
                                                           running_job_ids))
        if killing_jobs:
            LOGGER.info('killing %s', killing_jobs)

        res_data = {
            'new_job_available': has_task,
            'kill_job_ids': killing_jobs,
        }

        return self.send_json(res_data)


class GetNodeHandler(NodeBaseHandler):
    @authenticated
    def get(self, node_id):
        session = self.session
        node = self.node_manager.get_node(session=session, node_id=node_id)
        res_data = {
            'id': node.id,
            'tags': node.tags if node.tags else []
        }
        return self.send_json(res_data)


class NodeSessionInstanceNextjobHandler(NodeApiBaseHandler):
    @authenticated
    def post(self, node_session_id):
        node_session = self.get_node_session(node_session_id)
        session = self.session
        next_task = self.node_manager.node_get_next_job(session,
                                                        node_session.node)

        response_data = {'data': None}

        if not next_task:
            return self.write(json.dumps(response_data))

        spider = session.query(Spider) \
            .filter_by(id=next_task.spider_id) \
            .first()
        if not spider:
            LOGGER.error('Task %s has not spider, deleting.',
                         next_task.id)
            session.query(SpiderExecutionQueue) \
                .filter_by(id=next_task.id) \
                .delete()
            return self.write({'data': None})

        project = session.query(Project) \
            .filter_by(id=spider.project_id) \
            .first()
        if not project:
            LOGGER.error('Task %s has not project, deleting.',
                         next_task.id)
            session.query(SpiderExecutionQueue) \
                .filter_by(id=next_task.id) \
                .delete()
            return self.write({'data': None})

        extra_requirements_setting = session.query(SpiderSettings) \
            .filter_by(spider_id=spider.id,
                       setting_key='extra_requirements').first()

        if extra_requirements_setting and extra_requirements_setting.value:
            extra_requirements = [x for x
                                  in
                                  extra_requirements_setting.value.split(';')
                                  if x]
        else:
            extra_requirements = []
        task = {
            'task_id': next_task.id,
            'spider_id': next_task.spider_id,
            'spider_name': next_task.spider_name,
            'project_name': next_task.project_name,
            'version': project.version,
            'extra_requirements': extra_requirements,
            'spider_parameters': {parameter.parameter_key: parameter.value
                                  for parameter in spider.parameters},
            'figure': self.project_manager.get_job_figure(session, next_task),
        }

        figure = self.project_manager.get_job_figure(session, next_task)
        if figure:
            task['figure'] = figure.to_dict()

        LOGGER.debug('job_settings: %s', task)
        LOGGER.debug('next_task.settings: %s', next_task.settings)
        job_specific_settings = json.loads(next_task.settings) \
            if next_task.settings else {}
        if 'spider_parameters' in job_specific_settings:
            task['spider_parameters'] \
                .update(**job_specific_settings['spider_parameters'])
        LOGGER.debug('job_settings: %s', task)
        response_data['data'] = {'task': task}
        return self.write(json.dumps(response_data))


class ObtainNodeSessionJobHandler(NodeApiBaseHandler):
    @authenticated
    def post(self, node_session_id):
        node_session = self.get_node_session(node_session_id)
        session = self.session
        next_task = self.node_manager.node_get_next_job(session,
                                                        node_session.node)
        if not next_task:
            return self.set_status(404, 'No job available.')

        figure = self.project_manager.get_job_figure(session, next_task)
        task = {
            'name': 'nodeSessions/%s/jobs/%s' % (node_session.id,
                                                 next_task.id),
            'id': next_task.id,
            'figure': figure.to_json(),
        }
        return self.send_json(task)


class NodeSessionJobInstanceHandler(NodeApiBaseHandler):
    @authenticated
    def patch(self, node_session_id, job_id):
        node_session = self.get_node_session(node_session_id)
        status = self.get_argument('status')

        session = self.session
        job = session.query(SpiderExecutionQueue).get(job_id)
        if not job:
            raise tornado.web.HTTPError(404, 'job not found.')
        if node_session.node_id != job.node_id:
            raise tornado.web.HTTPError(404, 'job not found.')

        items_file = None
        if 'items' in self.request.files:
            items_file = BytesIO(self.request.files['items'][0].body)

        log_file = None
        if 'log' in self.request.files:
            log_file = BytesIO(self.request.files['log'][0].body)

        if status == 'success':
            status_int = 2
        elif status == 'fail':
            status_int = 3

        historical_job = self.node_manager.job_finish(session, job,
                                                        status_int,
                                                        log_file,
                                                        items_file)
        session.close()


class NodeCollectionHandler(NodeApiBaseHandler):
    def post(self):
        node_key = self.get_query_argument('node_key')
        session = self.session
        key = session.query(NodeKey).filter_by(key=node_key).first()
        if not key:
            raise tornado.web.HTTPError(400)

        if key.used_node_id:
            raise tornado.web.HTTPError(400)

        # supported array tags in formats of
        # 1 json :
        #    { "tags" : ["a", "b", "c"] }
        # 2 multi tags in urlencoded-from
        #    tags=a&tags=b&tags=c
        # 3 multi tags combined into one
        #    tags=a%2cb%2cc
        tags = self.get_arguments('tags') or []
        if len(tags) == 1 and ',' in tags[0]:
            tags = [tag for tag in tags[0].split(',') if tag]
        remote_ip = self.request.headers.get('X-Real-IP',
                                             self.request.remote_ip)
        node = self.node_manager.create_node(remote_ip, tags=tags,
                                             key_id=key.id)
        key.used_node_id = node.id
        session.add(key)
        session.commit()
        return self.send_json({
            'name': 'nodes/%s' % node.id,
            'id': node.id,
            'display_name': node.name,
            'tags': node.tags,
            'is_online': node.isalive > 0,
            'client_ip': node.client_ip
        })


class NodeInstanceHandler(NodeApiBaseHandler):
    @authenticated
    def get(self, node_id):
        node = self.node_manager.get_node(node_id, self.session)

        if not node:
            raise HTTPError(404, 'Object not found.')

        return self.send_json({
            'name': 'nodes/%s' % node.id,
            'id': node.id,
            'display_name': node.name,
            'tags': node.tags or [],
            'is_online': node.isalive > 0,
            'client_ip': node.client_ip
        })


class NodeSessionJobEggHandler(NodeApiBaseHandler):
    @authenticated
    def get(self, node_session_id, job_id):
        node_session = self.get_node_session(node_session_id)
        job = self.session.query(SpiderExecutionQueue) \
            .filter_by(id=job_id) \
            .first()
        if not job:
            raise tornado.web.HTTPError(404)

        if job.node_id != node_session.node_id:
            raise tornado.web.HTTPError(404)

        f_egg = self.project_manager.get_job_egg(self.session, job)
        self.set_header('content-type', 'application/octet-stream')
        self.write(f_egg.read())


class CompleteNodeSessionJobHandler(NodeApiBaseHandler):
    @authenticated
    def post(self, node_session_id, job_id):
        node_session_id = int(node_session_id)
        scheduler_manager = self.settings.get('scheduler_manager')
        status = self.get_body_argument('status')

        task_id = job_id
        if status == 'success':
            status_int = 2
        elif status == 'fail':
            status_int = 3
        else:
            self.set_status(400, 'Invalid argument: status.')
            return

        session = self.session
        node_session = self.node_manager.get_node_session(self.session,
                                                          node_session_id,
                                                          self.current_user)
        if not node_session:
            return self.set_status(401, 'Node session not found.')
        node_id = node_session.node_id
        query = session.query(SpiderExecutionQueue) \
            .filter(SpiderExecutionQueue.id == task_id,
                    SpiderExecutionQueue.status.in_([1, 5]))
        # be compatible with old agent version
        if node_id:
            query = query.filter(SpiderExecutionQueue.node_id == node_id)
        else:
            LOGGER.warning('Agent has not specified node id in '
                           'complete request, client address: %s.',
                           self.request.remote_ip)
        job = query.first()

        if job is None:
            self.set_status(404, 'Job not found.')
            session.close()
            return
        log_stream = BytesIO()
        try:
            logs_file = self.request.files['logs'][0]
            log_stream = BytesIO(logs_file['body'])
            log_stream.seek(0)
        except KeyError:
            pass
        items_stream = BytesIO()
        try:
            items_file = self.request.files['items'][0]
            items_stream = BytesIO(items_file['body'])
            items_stream.seek(0)
        except KeyError:
            pass

        job.status = status_int
        job.update_time = datetime.datetime.now()
        historical_job = scheduler_manager.job_finished(job,
                                                             log_stream,
                                                             items_stream)
        session.close()
        LOGGER.info('Job %s completed.', task_id)
        response_data = {'status': 'ok'}
        self.write(json.dumps(response_data))


url_patterns = [
    ('/v1/nodeSessions', NodeSessionListHandler),
    (r'/v1/nodeSessions/(\w+):heartbeat', NodeSessionInstanceHeartbeatHandler),
    (r'/v1/nodeSessions/(\w+):nextjob', NodeSessionInstanceNextjobHandler),
    (r'/v1/nodeSessions/(\w+)/jobs:obtain', ObtainNodeSessionJobHandler),
    (r'/v1/nodeSessions/(\w+)/jobs/(\w+)', NodeSessionJobInstanceHandler),
    (r'/v1/nodeSessions/(\w+)/jobs/(\w+)/egg', NodeSessionJobEggHandler),
    (r'/v1/nodeSessions/(\w+)/jobs/(\w+):complete', CompleteNodeSessionJobHandler),
    (r'/v1/nodes$', NodeCollectionHandler),
    (r'/v1/nodes/(\w+)$', NodeInstanceHandler),
]
