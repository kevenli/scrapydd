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
from .workspace import RunnerFactory, SpiderSetting
from .exceptions import ProcessFailed
from .security import generate_digest
from .poster.encode import multipart_encode
from .poster.streaminghttp import register_openers

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
        self.inner_client = AsyncHTTPClient(io_loop=io_loop,
                                            force_instance=force_instance,
                                            **kwargs)

    def fetch(self, request, callback=None, raise_error=True, **kwargs):
        if not isinstance(request, HTTPRequest):
            request = HTTPRequest(url=request, **kwargs)
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

        return self.inner_client.fetch(request, callback=callback,
                                       raise_error=raise_error, **kwargs)

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
        if os.path.exists('conf/node.conf'):
            parser = ConfigParser()
            parser.read('conf/node.conf')
            node_key = parser.get('agent', 'node_key')
            secret_key = parser.get('agent', 'secret_key')

        self.httpclient = NodeAsyncHTTPClient(self.service_base,
                                              key=node_key,
                                              secret_key=secret_key,
                                              defaults=httpclient_defaults)
        self.runner_factory = RunnerFactory(config)

    def start(self):
        self.register_node()
        # init heartbeat period callback
        heartbeat_callback = PeriodicCallback(self.send_heartbeat,
                                              self.heartbeat_interval)
        heartbeat_callback.start()


        # init checktask period callback
        # the new version use HEARTBEAT response to tell client whether there
        # is new task on server queue
        # so do not start this period method. But if server is an old version
        # without that header info
        # client sill need to poll GET_TASK.
        self.checktask_callback = PeriodicCallback(self.check_task,
                                                   self.checktask_interval)

        self.ioloop.start()

    def check_header_new_task_on_server(self, headers):
        try:
            new_task_on_server = headers['X-DD-New-Task'] == 'True'
            if new_task_on_server:
                self.ioloop.call_later(0, self.check_task)
        except KeyError:
            # if response contains no 'DD-New-Task' header, it might
            # be the old version server,
            # so use the old GET_TASK pool mode
            if not self.checktask_callback.is_running():
                self.checktask_callback.start()

    @coroutine
    def send_heartbeat(self):
        if self.status == EXECUTOR_STATUS_OFFLINE:
            self.register_node()
            return
        url = urljoin(self.service_base, '/nodes/%d/heartbeat' % self.node_id)
        running_tasks = ','.join([task_executor.task.id for task_executor in
                                  self.task_slots.tasks()])
        request = HTTPRequest(url=url, method='POST', body='',
                              headers={'X-DD-RunningJobs': running_tasks})
        try:
            res = yield self.httpclient.fetch(request)
            if 'X-DD-KillJobs' in res.headers:
                LOGGER.info('received kill signal %s',
                            res.headers['X-DD-KillJobs'])
                for job_id in json.loads(res.headers['X-DD-KillJobs']):
                    task_to_kill = self.task_slots.get_task(job_id)
                    if task_to_kill:
                        LOGGER.info('%s', task_to_kill)
                        task_to_kill.kill()
                        self.task_slots.remove_task(task_to_kill)
            self.check_header_new_task_on_server(res.headers)
        except HTTPError as ex:
            if ex.code == 400:
                logging.warning('Node expired, register now.')
                self.status = EXECUTOR_STATUS_OFFLINE
                self.register_node()
        except URLError as ex:
            logging.warning('Cannot connect to server. %s', ex)
        except Exception as ex:
            logging.warning('Cannot connect to server. %s', ex)

    @coroutine
    def register_node(self):
        if self.custom_ssl_cert and \
                self.service_base.startswith('https') and \
                not os.path.exists('keys/ca.crt'):
            httpclient = AsyncHTTPClient(force_instance=True)
            cacertrequest = HTTPRequest(urljoin(self.service_base, 'ca.crt'),
                                        validate_cert=False)
            try:
                cacertresponse = yield httpclient.fetch(cacertrequest)
                if not os.path.exists('keys'):
                    os.mkdir('keys')
                open('keys/ca.crt', 'wb').write(cacertresponse.body)
            except HTTPError:
                LOGGER.info('Custom ca cert retrieve failed.')
        try:

            node_id = yield self.httpclient.node_online(self.tags)
            self.status = EXECUTOR_STATUS_ONLINE
            self.node_id = node_id
            LOGGER.info('node %d registered', self.node_id)
        except URLError as ex:
            logging.warning('Cannot connect to server, %s', ex)
        except socket.error as ex:
            logging.warning('Cannot connect to server, %s', ex)

    def on_new_task_reach(self, task):
        if task is not None:
            task_executor = self.execute_task(task)
            self.task_slots.put_task(task_executor)

    @staticmethod
    def parse_task_data(response_data):
        task = SpiderTask()
        task.id = response_data['data']['task']['task_id']
        task.spider_id = response_data['data']['task']['spider_id']
        task.project_name = response_data['data']['task']['project_name']
        task.project_version = response_data['data']['task']['version']
        task.spider_name = response_data['data']['task']['spider_name']
        if 'extra_requirements' in response_data['data']['task'] and \
                response_data['data']['task']['extra_requirements']:
            task.extra_requirements = [x for x in
                                       response_data['data']['task'][
                                           'extra_requirements'].split(';') if
                                       x]
        if 'spider_parameters' in response_data['data']['task']:
            task.spider_parameters = response_data['data']['task'][
                'spider_parameters']
        else:
            task.spider_parameters = {}
        task.settings = response_data['data']
        return task

    @coroutine
    def get_next_task(self):
        url = urljoin(self.service_base, '/executing/next_task')
        post_data = urlencode({'node_id': self.node_id})
        request = HTTPRequest(url=url, method='POST', body=post_data)
        try:
            response = yield self.httpclient.fetch(request)
            response_content = response.body
            response_data = json.loads(response_content)
            LOGGER.debug(url)
            LOGGER.debug(response_content)
            if response_data['data'] is not None:
                task = Executor.parse_task_data(response_data)
                self.on_new_task_reach(task)
        except URLError:
            LOGGER.warning('Cannot connect to server')

    def execute_task(self, task):
        egg_downloader = ProjectEggDownloader(service_base=self.service_base,
                                              client=self.httpclient)
        executor = TaskExecutor(task, egg_downloader=egg_downloader,
                                runner_factory=self.runner_factory,
                                keep_files=self.keep_job_files)
        pid = None
        future = executor.execute()
        self.post_start_task(task, pid)
        executor.on_subprocess_start = self.post_start_task
        self.ioloop.add_future(future, self.task_finished)
        return executor

    @coroutine
    def post_start_task(self, task, pid):
        url = urljoin(self.service_base, '/jobs/%s/start' % task.id)
        post_data = urlencode({'pid': pid or ''})
        try:
            request = HTTPRequest(url=url, method='POST', body=post_data)
            yield self.httpclient.fetch(request)
        except URLError as ex:
            LOGGER.error('Error when post_task_task: %s', ex)
        except HTTPError as ex:
            LOGGER.error('Error when post_task_task: %s', ex)

    def complete_task(self, task_executor, status):
        """
        @type task_executor: TaskExecutor
        """
        url = urljoin(self.service_base, '/executing/complete')

        log_file = open(task_executor.output_file, 'rb')

        post_data = {
            'task_id': task_executor.task.id,
            'status': status,
            'log': log_file,
        }

        items_file = None
        if task_executor.items_file and \
                os.path.exists(task_executor.items_file):
            post_data['items'] = items_file = open(task_executor.items_file,
                                                   "rb")
            if LOGGER.isEnabledFor(logging.DEBUG):
                item_file_size = os.path.getsize(task_executor.items_file)
                LOGGER.debug('item file size : %d', item_file_size)
        LOGGER.debug(post_data)
        datagen, headers = multipart_encode(post_data)
        headers['X-DD-Nodeid'] = str(self.node_id)
        body_producer = MultipartRequestBodyProducer(datagen)
        request = HTTPRequest(url, method='POST', headers=headers,
                              body_producer=body_producer)
        client = self.httpclient
        future = client.fetch(request, raise_error=False)
        self.ioloop.add_future(future, self.complete_task_done(task_executor,
                                                               log_file,
                                                               items_file))
        LOGGER.info('task %s finished', task_executor.task.id)

    def complete_task_done(self, task_executor, log_file, items_file):
        def complete_task_done_f(future):
            """
            The callback of complete job request, if socket error occurred,
            retry commiting complete request in 10 seconds.
            If request is completed successfully, remove task from slots.
            Always close stream files.
            @type future: Future
            :return:
            """
            response = future.result()
            LOGGER.debug(response)
            if log_file:
                log_file.close()
            if items_file:
                items_file.close()
            if response.error and isinstance(response.error, socket.error):
                status = TASK_STATUS_SUCCESS if task_executor.ret_code == 0 \
                    else TASK_STATUS_FAIL
                self.ioloop.call_later(10, self.complete_task, task_executor,
                                       status)
                LOGGER.warning('Socket error when completing job, retry in '
                               '10 seconds.')
                return

            if response.error:
                LOGGER.warning('Error when post task complete request: %s',
                               response.error)
            self.task_slots.remove_task(task_executor)
            LOGGER.debug('complete_task_done')

        return complete_task_done_f

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
        self._spider_settings = SpiderSetting.from_dict(task.settings['task'])
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
            downloaded_egg = yield self.egg_downloader.download_egg_future(
                self.task.id)
            with open(downloaded_egg, 'rb') as egg_f:
                runner = self._runner_factory.build(egg_f)
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
            LOGGER.error(ex)
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
        self._f_output.write("Received a kill command, stopping spider.")
        self._runner.kill()


class ProjectEggDownloader:
    def __init__(self, service_base, client=None):
        self.download_path = None
        self.service_base = service_base
        if client is None:
            client = AsyncHTTPClient()
        self.client = client
        self._fd = None

    def download_egg_future(self, job_id):
        ret_future = Future()

        self.download_path = tempfile.mktemp()
        self._fd = open(self.download_path, 'wb')

        LOGGER.debug('begin download egg.')
        egg_request_url = urljoin(self.service_base, '/jobs/%s/egg' % job_id)
        request = HTTPRequest(egg_request_url,
                              streaming_callback=self._handle_chunk)
        client = self.client

        def done_callback(future):
            self._fd.close()
            ex = future.exception()
            if ex:
                return ret_future.set_exception(ex)

            return ret_future.set_result(self.download_path)

        client.fetch(request).add_done_callback(done_callback)
        return ret_future

    def _handle_chunk(self, chunk):
        self._fd.write(chunk)
        self._fd.flush()

    def __del__(self):
        if os.path.exists(self.download_path):
            os.remove(self.download_path)
