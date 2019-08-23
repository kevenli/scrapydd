from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.concurrent import Future
from tornado.gen import coroutine
import urllib2, urllib
import json
import os
import logging
from .config import AgentConfig
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
from .stream import MultipartRequestBodyProducer
import socket
from tornado import gen
from .workspace import ProjectWorkspace
import tempfile
import shutil
from .exceptions import *
from six.moves.urllib.parse import urlparse, urljoin, urlencode

logger = logging.getLogger(__name__)

TASK_STATUS_SUCCESS = 'success'
TASK_STATUS_FAIL = 'fail'

EXECUTOR_STATUS_OFFLINE = 0
EXECUTOR_STATUS_ONLINE = 1

from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
register_openers()


class SpiderTask():
    id = None
    spider_id = None
    project_name = None
    spider_name = None
    project_version = None
    spider_parameters = None
    extra_requirements = None


class TaskSlotContainer():
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
            raise ValueError('Task in TaskSlotContainer must be TaskExecutor type.')
        for i in range(len(self.slots)):
            if self.slots[i] is None:
                self.slots[i] = task
                break

    def remove_task(self, task):
        for i in range(len(self.slots)):
            if self.slots[i] == task:
                self.slots[i] = None
                break;

    def tasks(self):
        for item in self.slots:
            if item is not None:
                yield item

    def get_task(self, task_id):
        for item in self.slots:
            if item and item.task.id == task_id:
                return item

    @property
    def max_size(self):
        return self._max_size


class Executor():
    heartbeat_interval = 10
    checktask_interval = 10
    custom_ssl_cert = False

    def __init__(self, config=None):
        self.ioloop = IOLoop.current()
        self.node_id = None
        self.status = EXECUTOR_STATUS_OFFLINE
        self.tags = config.get('tags')

        if config is None:
            config =AgentConfig()
        self.task_slots = TaskSlotContainer(config.getint('slots', 1))
        self.config = config
        server_base = config.get('server')
        if urlparse(server_base).scheme == '':
            if config.getint('server_https_port'):
                server_base = 'https://%s:%d' % (server_base, config.getint('server_https_port'))
            else:
                server_base = 'http://%s:%d' % (server_base, config.getint('server_port'))
        self.service_base = server_base
        client_cert = config.get('client_cert') or None
        client_key = config.get('client_key') or None

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
        logger.debug(httpclient_defaults)
        self.httpclient = AsyncHTTPClient(defaults=httpclient_defaults)


    def start(self):
        self.register_node()
        #init heartbeat period callback
        heartbeat_callback = PeriodicCallback(self.send_heartbeat, self.heartbeat_interval*1000)
        heartbeat_callback.start()

        #init checktask period callback
        # the new version use HEARTBEAT response to tell client whether there is new task on server queue
        # so do not start this period method. But if server is an old version without that header info
        # client sill need to poll GET_TASK.
        self.checktask_callback = PeriodicCallback(self.check_task, self.checktask_interval*1000)



        self.ioloop.start()

    def check_header_new_task_on_server(self, headers):
        try:
            new_task_on_server = headers['X-DD-New-Task'] == 'True'
            if new_task_on_server:
                self.ioloop.call_later(0, self.check_task)
        except KeyError:
            # if response contains no 'DD-New-Task' header, it might be the old version server,
            # so use the old GET_TASK pool mode
            if not self.checktask_callback.is_running():
                self.checktask_callback.start()

    @coroutine
    def send_heartbeat(self):
        if self.status == EXECUTOR_STATUS_OFFLINE:
            self.register_node()
            return
        url = urljoin(self.service_base, '/nodes/%d/heartbeat' % self.node_id)
        running_tasks = ','.join([task_executor.task.id for task_executor in self.task_slots.tasks()])
        request = HTTPRequest(url=url, method='POST', body='', headers={'X-DD-RunningJobs': running_tasks})
        try:
            res = yield self.httpclient.fetch(request)
            if 'X-DD-KillJobs' in res.headers:
                logger.info('received kill signal %s' % res.headers['X-DD-KillJobs'])
                for job_id in json.loads(res.headers['X-DD-KillJobs']):
                    task_to_kill = self.task_slots.get_task(job_id)
                    if task_to_kill:
                        logger.info('%s' % task_to_kill)
                        task_to_kill.kill()
                        self.task_slots.remove_task(task_to_kill)
            self.check_header_new_task_on_server(res.headers)
        except urllib2.HTTPError as e:
            if e.code == 400:
                logging.warning('Node expired, register now.')
                self.status = EXECUTOR_STATUS_OFFLINE
                self.register_node()
        except urllib2.URLError as e:
            logging.warning('Cannot connect to server. %s' % e)
        except HTTPError as e:
            if e.code == 400:
                logging.warning('Node expired, register now.')
                self.status = EXECUTOR_STATUS_OFFLINE
                self.register_node()
        except Exception as e:
            logging.warning('Cannot connect to server. %s' % e)


    @coroutine
    def register_node(self):
        if self.custom_ssl_cert and self.service_base.startswith('https') and not os.path.exists('keys/ca.crt'):
            httpclient = AsyncHTTPClient(force_instance=True)
            cacertrequest = HTTPRequest(urljoin(self.service_base, 'ca.crt'), validate_cert=False)
            try:
                cacertresponse = yield httpclient.fetch(cacertrequest)
                if not os.path.exists('keys'):
                    os.mkdir('keys')
                open('keys/ca.crt', 'wb').write(cacertresponse.body)
            except HTTPError:
                logger.info('Custom ca cert retrieve failed.')
        try:
            url = urljoin(self.service_base, '/nodes')
            register_postdata = urllib.urlencode({'tags': self.tags})
            request = HTTPRequest(url=url, method='POST', body=register_postdata)
            res = yield self.httpclient.fetch(request)
            self.status = EXECUTOR_STATUS_ONLINE
            self.node_id = json.loads(res.body)['id']
            logger.info('node %d registered' % self.node_id)
        except urllib2.URLError as e:
            logging.warning('Cannot connect to server, %s' % e )
        except socket.error as e:
            logging.warning('Cannot connect to server, %s' % e)

    def on_new_task_reach(self, task):
        if task is not None:
            task_executor = self.execute_task(task)
            self.task_slots.put_task(task_executor)


    @coroutine
    def get_next_task(self):
        url = urljoin(self.service_base, '/executing/next_task')
        post_data = urllib.urlencode({'node_id': self.node_id})
        request = HTTPRequest(url=url, method='POST', body=post_data)
        try:
            response = yield self.httpclient.fetch(request)
            response_content = response.body
            response_data = json.loads(response_content)
            logger.debug(url)
            logger.debug(response_content)
            if response_data['data'] is not None:
                task = SpiderTask()
                task.id = response_data['data']['task']['task_id']
                task.spider_id = response_data['data']['task']['spider_id']
                task.project_name = response_data['data']['task']['project_name']
                task.project_version = response_data['data']['task']['version']
                task.spider_name = response_data['data']['task']['spider_name']
                if 'extra_requirements' in response_data['data']['task'] and \
                        response_data['data']['task']['extra_requirements']:
                    task.extra_requirements = [x for x in
                                               response_data['data']['task']['extra_requirements'].split(';') if x]
                if 'spider_parameters' in response_data['data']['task']:
                    task.spider_parameters = response_data['data']['task']['spider_parameters']
                else:
                    task.spider_parameters = {}
                self.on_new_task_reach(task)
        except urllib2.URLError:
            logger.warning('Cannot connect to server')

    def execute_task(self, task):
        egg_downloader = ProjectEggDownloader(service_base=self.service_base)
        executor = TaskExecutor(task, egg_downloader=egg_downloader)
        pid = None
        future = executor.execute()
        self.post_start_task(task, pid)
        executor.on_subprocess_start = self.post_start_task
        self.ioloop.add_future(future, self.task_finished)
        return executor

    @coroutine
    def post_start_task(self, task, pid):
        url = urljoin(self.service_base, '/jobs/%s/start' % task.id)
        post_data = urllib.urlencode({'pid':pid or ''})
        try:
            request = HTTPRequest(url=url, method='POST', body=post_data)
            respones = yield self.httpclient.fetch(request)
        except urllib2.URLError as e:
            logger.error('Error when post_task_task: %s' % e)
        except urllib2.HTTPError as e:
            logger.error('Error when post_task_task: %s' % e)

    def complete_task(self, task_executor, status):
        '''
        @type task_executor: TaskExecutor
        '''
        url = urljoin(self.service_base, '/executing/complete')

        log_file = open(task_executor.output_file, 'rb')

        post_data = {
            'task_id': task_executor.task.id,
            'status': status,
            'log': log_file,
        }

        items_file = None
        if task_executor.items_file and os.path.exists(task_executor.items_file):
            post_data['items'] = items_file = open(task_executor.items_file, "rb")
            logger.debug('item file size : %d' % os.path.getsize(task_executor.items_file))
        logger.debug(post_data)
        datagen, headers = multipart_encode(post_data)
        headers['X-DD-Nodeid'] = str(self.node_id)
        request = HTTPRequest(url, method='POST', headers=headers, body_producer=MultipartRequestBodyProducer(datagen))
        client = self.httpclient
        future = client.fetch(request, raise_error=False)
        self.ioloop.add_future(future, self.complete_task_done(task_executor, log_file, items_file))
        logger.info('task %s finished' % task_executor.task.id)


    def complete_task_done(self, task_executor, log_file, items_file):
        def complete_task_done_f(future):
            '''
            The callback of complete job request, if socket error occurred, retry commiting complete request in 10 seconds.
            If request is completed successfully, remove task from slots.
            Always close stream files.
            @type future: Future
            :return:
            '''
            response = future.result()
            logger.debug(response)
            if log_file:
                log_file.close()
            if items_file:
                items_file.close()
            if response.error and isinstance(response.error, socket.error):
                self.ioloop.call_later(10, self.complete_task, task_executor, TASK_STATUS_SUCCESS if task_executor.ret_code == 0 else TASK_STATUS_FAIL)
                logger.warning('Socket error when completing job, retry in 10 seconds.')
                return

            if response.error:
                logger.warning('Error when post task complete request: %s' % response.error)
            self.task_slots.remove_task(task_executor)
            logger.debug('complete_task_done')
        return complete_task_done_f

    def task_finished(self, future):
        task_executor = future.result()
        self.complete_task(task_executor, TASK_STATUS_SUCCESS if task_executor.ret_code == 0 else TASK_STATUS_FAIL)

    def check_task(self):
        if not self.task_slots.is_full():
            self.get_next_task()

class TaskExecutor():
    def __init__(self, task, egg_downloader):
        '''
        @type task: SpiderTask
        '''
        self.task = task
        self.egg_downloader = egg_downloader
        self._f_output = None
        self.output_file = None
        self.items_file = None
        self.ret_code = None
        self.workspace_dir = tempfile.mkdtemp(prefix='ddjob-%s-%s-' % (task.project_name, task.id))
        if not os.path.exists(self.workspace_dir):
            os.makedirs(self.workspace_dir)
        self.output_file = str(os.path.join(self.workspace_dir, '%s.log' % self.task.id))
        self._f_output = open(self.output_file, 'w')
        self.on_subprocess_start = None
        self.workspace = ProjectWorkspace(self.task.project_name, base_workdir=self.workspace_dir)

    @gen.coroutine
    def execute(self):
        try:
            yield self.workspace.init()
            downloaded_egg = yield self.egg_downloader.download_egg_future(self.task.id)
            with open(downloaded_egg, 'rb') as egg_f:
                self.workspace.put_egg(egg_f, self.task.project_version)
            logger.debug('download egg done.')
            yield self.workspace.install_requirements(self.task.extra_requirements)
            run_spider_future = self.workspace.run_spider(self.task.spider_name, self.task.spider_parameters, f_output=self._f_output, project=self.task.project_name)
            run_spider_pid = self.workspace.processes[0].pid
            if self.on_subprocess_start:
                self.on_subprocess_start(self.task, run_spider_pid)
            self.items_file = yield run_spider_future
            result = self.complete(0)
        except ProcessFailed as e:
            logger.warning('Process Failed when executing task %s: %s' % (self.task.id, e))
            error_log = e.message
            if e.std_output:
                logger.warning(e.std_output)
                error_log += e.std_output
            if e.err_output:
                logger.warning(e.err_output)
                error_log += e.err_output
            result = self.complete_with_error(error_log)
        except Exception as e:
            logger.error('Error when executing task %s: %s' % (self.task.id, e))
            logger.error(e)
            error_log = e.message
            result = self.complete_with_error(error_log)
        raise gen.Return(result)

    def result(self):
        return self

    def complete(self, ret_code):
        self._f_output.close()
        self.ret_code = ret_code
        return self.result()

    def complete_with_error(self, error_message):
        logger.debug(error_message)
        self._f_output.write(error_message)
        self._f_output.close()
        self.ret_code = 1
        return self.result()

    def __del__(self):
        logger.debug('delete task executor for task %s' % self.task.id)
        if self.workspace_dir and os.path.exists(self.workspace_dir):
            shutil.rmtree(self.workspace_dir)

    def kill(self):
        self._f_output.write("Received a kill command, stopping spider.")
        self.workspace.kill_process()


class ProjectEggDownloader(object):
    def __init__(self, service_base):
        self.download_path = None
        self.service_base = service_base

    def download_egg_future(self, job_id):
        ret_future = Future()

        self.download_path = tempfile.mktemp()
        self._fd = open(self.download_path, 'wb')

        logger.debug('begin download egg.')
        egg_request_url = urljoin(self.service_base, '/jobs/%s/egg' % job_id)
        request = HTTPRequest(egg_request_url, streaming_callback=self._handle_chunk)
        client = AsyncHTTPClient()
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
