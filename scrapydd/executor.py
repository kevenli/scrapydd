"""
This module is used by agent to execute spider task.
"""
# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
import json
import os
import logging
import socket
import tempfile
import shutil
from six.moves.urllib.parse import urlparse, urljoin, urlencode
from six.moves.urllib.error import URLError
from six.moves.configparser import ConfigParser
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.concurrent import Future
from tornado.gen import coroutine
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
from tornado import gen
from .config import AgentConfig
from .stream import MultipartRequestBodyProducer
from .workspace import RunnerFactory, SpiderSetting, DictSpiderSettings
from .exceptions import ProcessFailed
from .security import generate_digest
from .poster.encode import multipart_encode
from .poster.streaminghttp import register_openers
from .client import get_client, NoJobAvailable

LOGGER = logging.getLogger(__name__)

TASK_STATUS_SUCCESS = 'success'
TASK_STATUS_FAIL = 'fail'

EXECUTOR_STATUS_OFFLINE = 0
EXECUTOR_STATUS_ONLINE = 1

register_openers()


class NodeAsyncHTTPClient:
    def __init__(self, service_base, io_loop=None, force_instance=False,
                 **kwargs):
        self.service_base = service_base
        self.key = kwargs.pop('key', None)
        self.secret_key = kwargs.pop('secret_key', None)
        self.node_id = None
        if io_loop is None:
            io_loop = IOLoop.current()
        self.inner_client = AsyncHTTPClient(force_instance=force_instance,
                                            **kwargs)

    def fetch(self, request):
        if not isinstance(request, HTTPRequest):
            request = HTTPRequest(url=request)
        if self.key and self.secret_key:
            parsed_url = urlparse(request.url)
            path = parsed_url.path
            query = parsed_url.query
            method = request.method
            body = request.body or b''
            digest = generate_digest(self.secret_key, method, path, query,
                                     body)
            authorization_header = '%s %s %s' % ('HMAC', self.key, digest)
            request.headers['Authorization'] = authorization_header
        if self.node_id:
            request.headers['X-Dd-Nodeid'] = str(self.node_id)

        return self.inner_client.fetch(request)

    @coroutine
    def node_online(self, tags):
        url = urljoin(self.service_base, '/nodes')
        register_postdata = urlencode({'tags': tags})
        request = HTTPRequest(url=url, method='POST', body=register_postdata)

        response = yield self.fetch(request)
        self.node_id = json.loads(response.body)['id']
        raise gen.Return(self.node_id)


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


class TaskSlotContainer:
    """
    Slot container to control concurrency running on agent side.
    Agent need allocate a slot before running.
    """
    def __init__(self, max_size=1):
        self._max_size = max_size
        self.slots = [None] * max_size

    def is_full(self):
        for slot in self.slots:
            if slot is None:
                return False
        return True

    def put_task(self, task):
        if not isinstance(task, TaskExecutor):
            raise ValueError('Task in TaskSlotContainer must be '
                             'TaskExecutor type.')
        for i in range(len(self.slots)):
            if self.slots[i] is None:
                self.slots[i] = task
                break

    def remove_task(self, task):
        for i in range(len(self.slots)):
            if self.slots[i] == task:
                self.slots[i] = None
                break

    def tasks(self):
        for item in self.slots:
            if item is not None:
                yield item

    def get_task(self, task_id):
        for item in self.slots:
            if item and item.task.id == task_id:
                return item
        return None

    @property
    def max_size(self):
        return self._max_size


class Executor:
    """
    Main object communicate to server and control tasks running.
    """
    # hearbeat check interval, default: 10 seconds
    heartbeat_interval = 10 * 1000
    # check task interval, default: 10 seconds
    checktask_interval = 10 * 1000
    custom_ssl_cert = False

    def __init__(self, config=None):

        self.ioloop = IOLoop.current()
        self.node_id = None
        self.status = EXECUTOR_STATUS_OFFLINE
        if config is None:
            config = AgentConfig()
        self.config = config
        self.tags = config.get('tags')
        self.checktask_callback = None

        self.task_slots = TaskSlotContainer(config.getint('slots', 1))

        server_base = config.get('server')
        if urlparse(server_base).scheme == '':
            if config.getint('server_https_port'):
                server_https_port = config.getint('server_https_port')
                server_base = 'https://%s:%d' % (server_base,
                                                 server_https_port)
            else:
                server_base = 'http://%s:%d' % (server_base,
                                                config.getint('server_port'))
        self.service_base = server_base
        client_cert = config.get('client_cert') or None
        client_key = config.get('client_key') or None
        self.keep_job_files = config.getboolean('debug', False)
        LOGGER.debug('keep_job_files %s', self.keep_job_files)

        httpclient_defaults = {
            'request_timeout': config.getfloat('request_timeout', 60)
        }
        if client_cert:
            httpclient_defaults['client_cert'] = client_cert
        if client_key:
            httpclient_defaults['client_key'] = client_key
        if os.path.exists('keys/ca.crt'):
            self.custom_ssl_cert = True
            httpclient_defaults['ca_certs'] = 'keys/ca.crt'
            httpclient_defaults['validate_cert'] = True
        LOGGER.debug(httpclient_defaults)

        node_key = None
        secret_key = None
        node_id = None
        if os.path.exists('conf/node.conf'):
            parser = ConfigParser()
            parser.read('conf/node.conf')
            node_key = parser.get('agent', 'node_key')
            secret_key = parser.get('agent', 'secret_key')
            node_id = int(parser.get('agent', 'node_id'))

        self.httpclient = NodeAsyncHTTPClient(self.service_base,
                                              key=node_key,
                                              secret_key=secret_key,
                                              defaults=httpclient_defaults)
        self.client = get_client(config,
                                 app_key=node_key,
                                 app_secret=secret_key,
                                 node_id=node_id)
        self.runner_factory = RunnerFactory(config)

    def start(self):
        self.register_node()
        # init heartbeat period callback
        heartbeat_callback = PeriodicCallback(self.send_heartbeat,
                                              self.heartbeat_interval)
        heartbeat_callback.start()

        self.ioloop.start()

    @coroutine
    def send_heartbeat(self):
        if self.status == EXECUTOR_STATUS_OFFLINE:
            self.register_node()
            return
        running_job_ids = [task_executor.task.id for task_executor in
                                   self.task_slots.tasks()]
        res = self.client.heartbeat(running_job_ids=running_job_ids)
        for job_id in res['kill_job_ids'] or []:
            task_to_kill = self.task_slots.get_task(job_id)
            if task_to_kill:
                LOGGER.info('%s', task_to_kill)
                task_to_kill.kill()
                self.task_slots.remove_task(task_to_kill)

        if res['new_job_available']:
            self.ioloop.call_later(0, self.check_task)

    def register_node(self):
        self.client.login()
        self.status = EXECUTOR_STATUS_ONLINE
        self.node_id = self.client._node_id
        self.httpclient.node_id = self.node_id

    def on_new_task_reach(self, task):
        if task is not None:
            task_executor = self.execute_task(task)
            self.task_slots.put_task(task_executor)

    @coroutine
    def get_next_task(self):
        try:
            task = self.client.get_next_job()
            self.on_new_task_reach(task)
        except NoJobAvailable as ex:
            LOGGER.warn('NoJobAvailable')
        except Exception as ex:
            LOGGER.error(ex)

    def execute_task(self, task):
        package_egg = self.client.get_job_egg(task.id)
        executor = TaskExecutor(task, egg_downloader=package_egg,
                                runner_factory=self.runner_factory,
                                keep_files=self.keep_job_files)
        future = executor.execute()
        self.ioloop.add_future(future, self.task_finished)
        return executor

    def complete_task(self, task_executor, status):
        """
        @type task_executor: TaskExecutor
        """
        self.client.complete_job(task_executor.task.id, status,
                                 items_file=task_executor.items_file,
                                 logs_file=task_executor.output_file)
        self.task_slots.remove_task(task_executor)

    def task_finished(self, future):
        task_executor = future.result()
        status = TASK_STATUS_SUCCESS if task_executor.ret_code == 0 \
            else TASK_STATUS_FAIL
        self.complete_task(task_executor, status)

    def check_task(self):
        if not self.task_slots.is_full():
            self.get_next_task()


class TaskExecutor:
    def __init__(self, task, egg_downloader, runner_factory, keep_files=False):
        """
        @type task: SpiderTask
        @type egg_downloader: ProjectEggDownloader
        @type runner_factory: RunnerFactory
        """
        self.task = task
        self.egg_downloader = egg_downloader
        self._runner_factory = runner_factory
        self._spider_settings = DictSpiderSettings(task.figure or
                                                   task.settings['task'])
        self._f_output = None
        self.output_file = None
        self.items_file = None
        self.ret_code = None
        prefix = 'ddjob-%s-%s-' % (task.project_name, task.id)
        self.workspace_dir = tempfile.mkdtemp(prefix=prefix)
        if not os.path.exists(self.workspace_dir):
            os.makedirs(self.workspace_dir)
        self.output_file = str(os.path.join(self.workspace_dir, 'crawl.log'))
        self._f_output = open(self.output_file, 'w')
        self.on_subprocess_start = None
        self.keep_files = keep_files
        self._runner = None

    @gen.coroutine
    def execute(self):
        try:
            LOGGER.info('start fetch spider egg.')
            runner = self._runner_factory.build(self.egg_downloader)
            self._runner = runner
            crawl_result = yield runner.crawl(self._spider_settings)
            self.items_file = crawl_result.items_file
            self.output_file = crawl_result.crawl_logfile
            result = self.complete(0)
        except ProcessFailed as ex:
            LOGGER.warning('Process Failed when executing task %s: %s',
                           self.task.id, ex)
            error_log = ex.message
            if ex.std_output:
                LOGGER.warning(ex.std_output)
                error_log += ex.std_output
            if ex.err_output:
                LOGGER.warning(ex.err_output)
                error_log += ex.err_output
            result = self.complete_with_error(error_log)
        except Exception as ex:
            LOGGER.error('Error when executing task %s: %s', self.task.id, ex)
            error_log = str(ex)
            result = self.complete_with_error(error_log)
        raise gen.Return(result)

    def result(self):
        return self

    def complete(self, ret_code):
        self._f_output.close()
        self.ret_code = ret_code
        return self.result()

    def complete_with_error(self, error_message):
        LOGGER.debug(error_message)
        self._f_output.write(error_message)
        self._f_output.close()
        self.ret_code = 1
        return self.result()

    def __del__(self):
        if not self.keep_files:
            LOGGER.debug('delete task executor for task %s', self.task.id)
            if self.workspace_dir and os.path.exists(self.workspace_dir):
                shutil.rmtree(self.workspace_dir)
            if self._runner:
                self._runner.clear()
                self._runner = None

    def kill(self):
        if not self._f_output.closed:
            self._f_output.write("Received a kill command, stopping spider.")
        self._runner.kill()
