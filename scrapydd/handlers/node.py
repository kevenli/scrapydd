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
from tornado.web import authenticated
from six import ensure_str, binary_type
from .base import RestBaseHandler
from ..models import session_scope, Session, Spider, Project
from ..models import SpiderExecutionQueue, SpiderSettings, NodeKey
from ..stream import PostDataStreamer
from ..exceptions import NodeExpired
from ..security import generate_digest
from ..storage import ProjectStorage

LOGGER = logging.getLogger(__name__)


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
            LOGGER.info("Invalid Authorization header %s", authorization)
            return None

        algorithm, key, provided_digest = authorization
        if algorithm != "HMAC":
            LOGGER.info("Invalid algorithm %s", algorithm)
            return None

        with session_scope() as session:
            node_key = session.query(NodeKey).filter_by(key=key).first()

            if node_key is None:
                LOGGER.info("Invalid HMAC key %s", key)
                return None
            secret = node_key.secret_key
            body = handler.request.body if isinstance(handler.request.body,
                                                      binary_type) else b''
            expected_digest = generate_digest(
                secret, handler.request.method, handler.request.path,
                handler.request.query,
                body)

            if not hmac.compare_digest(expected_digest, provided_digest):
                LOGGER.info("Invalid HMAC digest %s", provided_digest)
                return None
            return node_key


class NodeBaseHandler(RestBaseHandler):
    authentication_providers = [NodeHmacAuthenticationProvider()]

    def get_current_user(self):
        # if enable_node_registration is not enabled, node do not
        # need register to work.
        # get node_id from request
        if not self.settings.get('enable_node_registration', False):
            return int(self.request.headers.get('X-Dd-Nodeid'))

        for authentication_provider in self.authentication_providers:
            node_id = authentication_provider.get_user(self)
            return node_id


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
            remote_ip = self.request.remote_ip
            node_manager = self.settings.get('node_manager')
            node = node_manager.create_node(remote_ip, tags=tags,
                                            key_id=node_key.id)
            node_key.used_node_id = node.id
            session.add(node_key)
            session.commit()
            return self.send_json({'id': node.id})


class NodesHandler(NodeBaseHandler):
    # pylint: disable=arguments-differ
    def initialize(self, node_manager):
        super(NodesHandler, self).initialize()
        self.node_manager = node_manager

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
        if node_id:
            node = self.node_manager.get_node(node_id)
            with session_scope() as session:
                node.tags = tags
                node.isalive = True
                node.last_heartbeat = datetime.datetime.now()
                session.add(node)
                session.commit()
        else:
            node = self.node_manager.create_node(remote_ip, tags=tags)
        return self.write(json.dumps({'id': node.id}))


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

            extra_requirements = extra_requirements_setting.value \
                if extra_requirements_setting else ''
            task = {
                'task_id': next_task.id,
                'spider_id': next_task.spider_id,
                'spider_name': next_task.spider_name,
                'project_name': next_task.project_name,
                'version': project.version,
                'extra_requirements': extra_requirements,
                'spider_parameters': {parameter.parameter_key: parameter.value
                                      for parameter in spider.parameters},
            }
            if project.package:
                task['base_settings_module'] = project.package.settings_module
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
        log_stream = None
        try:
            spider_log_folder = os.path.join('logs', job.project_name,
                                             job.spider_name)
            if not os.path.exists(spider_log_folder):
                os.makedirs(spider_log_folder)

            log_part = self.ps.get_parts_by_name('log')[0]
            if log_part:
                log_stream = open(log_part['tmpfile'].name, 'rb')

        except Exception as ex:
            LOGGER.error('Error when writing task log file, %s', ex)

        items_parts = self.ps.get_parts_by_name('items')
        items_stream = None
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
                items_stream = open(tmpfile, 'rb')
            except Exception as ex:
                LOGGER.error('Error when writing items file, %s', ex)

        job.status = status_int
        job.update_time = datetime.datetime.now()
        historical_job = self.scheduler_manager.job_finished(job,
                                                             log_stream,
                                                             items_stream)

        if items_file:
            self.webhook_daemon.on_spider_complete(historical_job,
                                                   items_file)

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
        self.node_manager = node_manager
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
        running_job_ids = [int(x) for x in running_jobs.split(',') if x]
        if running_jobs:
            killing_jobs = self.scheduler_manager.jobs_running(node_id,
                                                               running_job_ids)
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
        with session_scope() as session:
            job = session.query(SpiderExecutionQueue)\
                .filter_by(id=jobid)\
                .first()
            if not job:
                raise tornado.web.HTTPError(404)
            project = job.spider.project
            project_storage_dir = self.settings.get('project_storage_dir')
            project_storage = ProjectStorage(project_storage_dir, project)
            version, f_egg = project_storage.get_egg()
            LOGGER.debug('get project version, project id: %s version: %s',
                         project.id, version)
            self.write(f_egg.read())
            session.close()
