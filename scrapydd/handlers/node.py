from .base import RestBaseHandler
from ..models import session_scope, Session, Spider, Project, SpiderExecutionQueue, SpiderSettings, NodeKey
import json
import logging
import tornado
import datetime
import os
from ..stream import PostDataStreamer
from ..exceptions import *
from six import ensure_str, binary_type
from six.moves.urllib.parse import urlparse, urlencode
from ..security import generate_digest
from tornado.web import HTTPError
import hmac
from ..eggstorage import FilesystemEggStorage
import functools

logger = logging.getLogger(__name__)


def authenticated(method):
    """Decorate methods with this to require that the user be logged in.

    If the user is not logged in, they will be redirected to the configured
    `login url <RequestHandler.get_login_url>`.

    If you configure a login url with a query parameter, Tornado will
    assume you know what you're doing and use it as-is.  If not, it
    will add a `next` parameter so the login page knows where to send
    you once you're logged in.
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.current_user:
            raise HTTPError(403)
        return method(self, *args, **kwargs)
    return wrapper

class NodeHmacAuthenticationProvider(object):
    def get_user(self, handler):
        authorization = handler.request.headers.get("Authorization", "").split(" ")
        if len(authorization) != 3:
            logging.info("Invalid Authorization header {}".format(authorization))
            return None

        algorithm, key, provided_digest = authorization
        if algorithm != "HMAC":
            logging.info("Invalid algorithm {}".format(algorithm))
            return None

        with session_scope() as session:
            node_key = session.query(NodeKey).filter_by(key=key).first()

            if node_key is None:
                logging.info("Invalid HMAC key {}".format(key))
                return None
            secret = node_key.secret_key
            body = handler.request.body if isinstance(handler.request.body, binary_type) else b''
            expected_digest = generate_digest(
                secret, handler.request.method, handler.request.path, handler.request.query,
                body)


            if not hmac.compare_digest(expected_digest, provided_digest):
                logging.info("Invalid HMAC digest {}".format(provided_digest))
                return None

            return node_key.used_node_id



class NodeBaseHandler(RestBaseHandler):
    authentication_providers = [NodeHmacAuthenticationProvider()]

    def get_current_user(self):
        # if enable_node_registration is not enabled, node do not need register to work.
        # get node_id from request
        if not self.settings.get('enable_node_registration', False):
            return int(self.request.headers.get('X-Dd-Nodeid'))

        for authentication_provider in self.authentication_providers:
            node_id = authentication_provider.get_user(self)
            return node_id


class RegisterNodeHandler(NodeBaseHandler):
    @authenticated
    def post(self):
        node_key = self.current_user
        with session_scope() as session:
            tags = self.get_argument('tags', '').strip()
            tags = None if tags == '' else tags
            remote_ip = self.request.remote_ip
            node_manager = self.settings.get('node_manager')
            node = node_manager.create_node(remote_ip, tags=tags, key_id=node_key.id)
            node_key.used_node_id = node.id
            session.add(node_key)
            session.commit()
            self.send_json({'id': node.id})

    def get_current_user(self):
        authorization = self.request.headers.get("Authorization", "").split(" ")
        if len(authorization) != 3:
            logging.info("Invalid Authorization header {}".format(authorization))
            return None

        algorithm, key, provided_digest = authorization
        if algorithm != "HMAC":
            logging.info("Invalid algorithm {}".format(algorithm))
            return None

        with session_scope() as session:
            user_key = session.query(NodeKey).filter_by(key=key).first()

            if user_key is None:
                logging.info("Invalid HMAC key {}".format(key))
                return None

            if user_key.is_deleted:
                logging.info("Invalid HMAC key {}".format(key))
                return None

            secret = user_key.secret_key
            expected_digest = generate_digest(
                secret, self.request.method, self.request.path, self.request.query,
                self.request.body)

            if not hmac.compare_digest(expected_digest, provided_digest):
                logging.info("Invalid HMAC digest {}".format(provided_digest))
                return None

            return user_key


class NodesHandler(NodeBaseHandler):
    def initialize(self, node_manager):
        super(NodesHandler, self).initialize()
        self.node_manager = node_manager

    def post(self):
        enable_node_registration = self.settings.get('enable_node_registration', False)
        node_id = NodeHmacAuthenticationProvider().get_user(self)

        # if node_registerion is enabled, node should be assigned node_id before use by
        # invoke /nodes/register
        logger.debug('enable_node_registration : %s' % enable_node_registration)
        if enable_node_registration and node_id is None:
            return self.set_status(403, 'node registration is enabled, no key provided.')

        tags = self.get_argument('tags', '').strip()
        tags = None if tags == '' else tags
        remote_ip = self.request.headers.get('X-Real-IP') or self.request.remote_ip
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
        self.write(json.dumps({'id': node.id}))


class ExecuteNextHandler(NodeBaseHandler):
    scheduler_manager = None

    def initialize(self, scheduler_manager=None):
        self.scheduler_manager = scheduler_manager

    @authenticated
    def post(self):
        with session_scope() as session:
            node_id = int(self.get_argument('node_id'))
            next_task = self.scheduler_manager.get_next_task(node_id)

            response_data = {'data': None}

            if next_task is not None:
                spider = session.query(Spider).filter_by(id=next_task.spider_id).first()
                if not spider:
                    logger.error('Task %s has not spider, deleting.' % next_task.id)
                    session.query(SpiderExecutionQueue).filter_by(id=next_task.id).delete()
                    self.write({'data': None})
                    return

                project = session.query(Project).filter_by(id=spider.project_id).first()
                if not project:
                    logger.error('Task %s has not project, deleting.' % next_task.id)
                    session.query(SpiderExecutionQueue).filter_by(id=next_task.id).delete()
                    self.write({'data': None})
                    return

                extra_requirements_setting = session.query(SpiderSettings) \
                    .filter_by(spider_id=spider.id, setting_key='extra_requirements').first()

                extra_requirements = extra_requirements_setting.value if extra_requirements_setting else ''
                response_data['data'] = {'task': {
                    'task_id': next_task.id,
                    'spider_id': next_task.spider_id,
                    'spider_name': next_task.spider_name,
                    'project_name': next_task.project_name,
                    'version': project.version,
                    'extra_requirements': extra_requirements,
                    'spider_parameters': {parameter.parameter_key: parameter.value for parameter in spider.parameters}
                }}
            self.write(json.dumps(response_data))


MB = 1024 * 1024
GB = 1024 * MB
TB = 1024 * GB
MAX_STREAMED_SIZE = 1 * GB


@tornado.web.stream_request_body
class ExecuteCompleteHandler(NodeBaseHandler):
    webhook_daemon = None
    scheduler_manager = None
    ps = None

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
        self.ps = PostDataStreamer(total)  # ,tmpdir="/tmp"

        # self.fout = open("raw_received.dat","wb+")

    @authenticated
    def post(self):
        try:
            self.ps.finish_receive()
            fields = self.ps.get_values(['task_id', 'status'])
            logger.debug(self.ps.get_nonfile_names())
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
                .filter(SpiderExecutionQueue.id == task_id, SpiderExecutionQueue.status.in_([1,5]))
            # be compatible with old agent version
            if node_id:
                query = query.filter(SpiderExecutionQueue.node_id == node_id)
            else:
                logger.warning('Agent has not specified node id in complete request, client address: %s.' %
                               self.request.remote_ip)
            job = query.first()

            if job is None:
                self.set_status(404, 'Job not found.')
                session.close()
                return
            log_file = None
            items_file = None
            try:
                spider_log_folder = os.path.join('logs', job.project_name, job.spider_name)
                if not os.path.exists(spider_log_folder):
                    os.makedirs(spider_log_folder)

                log_part = self.ps.get_parts_by_name('log')[0]
                if log_part:
                    import shutil
                    log_file = os.path.join(spider_log_folder, job.id + '.log')
                    shutil.copy(log_part['tmpfile'].name, log_file)

            except Exception as e:
                logger.error('Error when writing task log file, %s' % e)

            items_parts = self.ps.get_parts_by_name('items')
            if items_parts:
                try:
                    part = items_parts[0]
                    tmpfile = part['tmpfile'].name
                    logger.debug('tmpfile size: %d' % os.path.getsize(tmpfile))
                    items_file_path = os.path.join('items', job.project_name, job.spider_name)
                    if not os.path.exists(items_file_path):
                        os.makedirs(items_file_path)
                    items_file = os.path.join(items_file_path, '%s.jl' % job.id)
                    import shutil
                    shutil.copy(tmpfile, items_file)
                    logger.debug('item file size: %d' % os.path.getsize(items_file))
                except Exception as e:
                    logger.error('Error when writing items file, %s' % e)

            job.status = status_int
            job.update_time = datetime.datetime.now()
            historical_job = self.scheduler_manager.job_finished(job, log_file, items_file)

            if items_file:
                self.webhook_daemon.on_spider_complete(historical_job, items_file)

            session.close()
            logger.info('Job %s completed.' % task_id)
            response_data = {'status': 'ok'}
            self.write(json.dumps(response_data))

        finally:
            # Don't forget to release temporary files.
            self.ps.release_parts()

    def data_received(self, chunk):
        self.ps.receive(chunk)


class NodeHeartbeatHandler(NodeBaseHandler):
    def initialize(self, node_manager, scheduler_manager):
        super(NodeHeartbeatHandler, self).initialize()
        self.node_manager = node_manager
        self.scheduler_manager = scheduler_manager

    @authenticated
    def post(self, id):
        # logger.debug(self.request.headers)
        node_id = int(self.current_user)
        self.set_header('X-DD-New-Task', self.scheduler_manager.has_task(node_id))
        try:
            self.node_manager.heartbeat(node_id)
            running_jobs = self.request.headers.get('X-DD-RunningJobs', None)
            if running_jobs:
                killing_jobs = list(self.scheduler_manager.jobs_running(node_id, running_jobs.split(',')))
                if killing_jobs:
                    logger.info('killing %s' % killing_jobs)
                    self.set_header('X-DD-KillJobs', json.dumps(killing_jobs))
            response_data = {'status': 'ok'}
        except NodeExpired:
            response_data = {'status': 'error', 'errmsg': 'Node expired'}
            self.set_status(400, 'Node expired')
        self.write(json.dumps(response_data))


class JobStartHandler(NodeBaseHandler):
    def initialize(self, scheduler_manager):
        super(JobStartHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    @authenticated
    def post(self, jobid):
        pid = self.get_argument('pid')
        pid = int(pid) if pid else None
        self.scheduler_manager.job_start(jobid, pid)


class JobEggHandler(NodeBaseHandler):
    @authenticated
    def get(self, jobid):
        with session_scope() as session:
            job = session.query(SpiderExecutionQueue).filter_by(id=jobid).first()
            if not job:
                raise tornado.web.HTTPError(404)
            spider = session.query(Spider).filter_by(id=job.spider_id).first()
            storage = FilesystemEggStorage({})
            version, f = storage.get(spider.project.name)
            self.write(f.read())
            session.close()

