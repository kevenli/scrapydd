from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.concurrent import Future
from tornado.gen import coroutine
import urllib2, urllib
import json
from scrapyd.eggstorage import FilesystemEggStorage
import scrapyd.config
import subprocess
import os
import urlparse
import logging
from config import AgentConfig
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
from stream import MultipartRequestBodyProducer
from w3lib.url import path_to_file_uri
import socket
from tornado import gen
from workspace import ProjectWorkspace
import tempfile
import shutil
from exceptions import *

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
        if not isinstance(task, SpiderTask):
            raise ValueError('Task in TaskSlotContainer must be SpiderTask type.')
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

    @property
    def max_size(self):
        return self._max_size

class Executor():
    heartbeat_interval = 10
    checktask_interval = 10

    def __init__(self, config=None):
        self.ioloop = IOLoop.current()
        self.node_id = None
        self.status = EXECUTOR_STATUS_OFFLINE

        if config is None:
            config =AgentConfig()
        self.task_slots = TaskSlotContainer(config.getint('slots', 1))
        self.config = config
        # if server_https_port is configured, prefer to use it.
        if config.get('server_https_port'):
            self.service_base = 'https://%s:%d'% (config.get('server'), config.getint('server_https_port'))
        else:
            self.service_base = 'http://%s:%d' % (config.get('server'), config.getint('server_port'))
        client_cert = config.get('client_cert') or None
        client_key = config.get('client_key') or None

        self.httpclient = AsyncHTTPClient(defaults=dict(validate_cert=True, ca_certs='keys/ca.crt', client_cert=client_cert, client_key=client_key))


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

        # code for debuging memory leak
        # import objgraph
        # def check_memory():
        #     logger.debug('Checking memory.')
        #     logger.debug(objgraph.by_type('SpiderTask'))
        #     logger.debug(objgraph.by_type('TaskExecutor'))
        #     logger.debug(objgraph.by_type('Future'))
        #     logger.debug(objgraph.by_type('PeriodicCallback'))
        #     future_objs = objgraph.by_type('Future')
        #     if future_objs:
        #         objgraph.show_chain(
        #             objgraph.find_backref_chain(future_objs[-1], objgraph.is_proper_module),
        #             filename='chain.png'
        #         )
        # self.check_memory_callback = PeriodicCallback(check_memory, 1*1000)
        # self.check_memory_callback.start()

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
        url = urlparse.urljoin(self.service_base, '/nodes/%d/heartbeat' % self.node_id)
        running_tasks = ','.join([task.id for task in self.task_slots.tasks()])
        request = HTTPRequest(url=url, method='POST', body='', headers={'X-DD-RunningJobs': running_tasks})
        try:
            res = yield self.httpclient.fetch(request)
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
        if self.service_base.startswith('https') and not os.path.exists('keys/ca.crt') :
            httpclient = AsyncHTTPClient(force_instance=True)
            cacertrequest = HTTPRequest(urlparse.urljoin(self.service_base, 'ca.crt'), validate_cert=False)
            cacertresponse = yield httpclient.fetch(cacertrequest)
            if not os.path.exists('keys'):
                os.mkdir('keys')
            open('keys/ca.crt', 'wb').write(cacertresponse.body)
        try:
            url = urlparse.urljoin(self.service_base, '/nodes')
            request = HTTPRequest(url = url, method='POST', body='')
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
            self.task_slots.put_task(task)
            self.execute_task(task)

    @coroutine
    def get_next_task(self):
        url = urlparse.urljoin(self.service_base, '/executing/next_task')
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
                self.on_new_task_reach(task)
        except urllib2.URLError:
            logger.warning('Cannot connect to server')

    def execute_task(self, task):
        executor = TaskExecutor(task, config=self.config)
        pid = None
        future = executor.execute()
        self.post_start_task(task, pid)
        executor.on_subprocess_start = self.post_start_task
        self.ioloop.add_future(future, self.task_finished)

    @coroutine
    def post_start_task(self, task, pid):
        url = urlparse.urljoin(self.service_base, '/jobs/%s/start' % task.id)
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
        url = urlparse.urljoin(self.service_base, '/executing/complete')

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
        request = HTTPRequest(url, method='POST', headers=headers, request_timeout=60, body_producer=MultipartRequestBodyProducer(datagen))
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
            self.task_slots.remove_task(task_executor.task)
            logger.debug('complete_task_done')
        return complete_task_done_f

    def task_finished(self, future):
        task_executor = future.result()
        self.complete_task(task_executor, TASK_STATUS_SUCCESS if task_executor.ret_code == 0 else TASK_STATUS_FAIL)

    def check_task(self):
        if not self.task_slots.is_full():
            self.get_next_task()

class TaskExecuteResult():
    task = None
    output_file = None
    items_file = None
    ret_code = None
    executor = None

    def __init__(self, task, log_file, items_file, ret_code):
        self.task = task
        self.output_file = log_file
        self.items_file = items_file
        self.ret_code = ret_code

class TaskExecutor():
    def __init__(self, task, config=None):
        '''
        @type task: SpiderTask
        '''
        self.task = task
        if config is None:
            config = AgentConfig()
        if config.get('server_https_port'):
            self.service_base = 'https://%s:%d' % (config.get('server'), config.getint('server_https_port'))
        else:
            self.service_base = 'http://%s:%d' % (config.get('server'), config.getint('server_port'))
        self._f_output = None
        self.output_file = None
        self.p = None
        self.check_process_callback = None
        self.items_file = None
        self.ret_code = None
        self.workspace_dir = tempfile.mkdtemp(prefix='ddjob-%s-%s-' % (task.project_name, task.id))
        if not os.path.exists(self.workspace_dir):
            os.makedirs(self.workspace_dir)
        self.output_file = str(os.path.join(self.workspace_dir, '%s.log' % self.task.id))
        self._f_output = open(self.output_file, 'w')

        eggs_dir = os.path.join(self.workspace_dir, 'eggs')
        if not os.path.exists(eggs_dir):
            os.mkdir(eggs_dir)
        self.egg_storage = FilesystemEggStorage(scrapyd.config.Config(values={'eggs_dir': eggs_dir}))
        self.on_subprocess_start = None

    @gen.coroutine
    def execute(self):
        try:
            workspace = ProjectWorkspace(self.task.project_name)
            yield workspace.init()
            logger.debug('begin download egg.')
            egg_request_url = urlparse.urljoin(self.service_base, '/spiders/%d/egg' % self.task.spider_id)
            request = HTTPRequest(egg_request_url)
            client = AsyncHTTPClient()
            response = yield client.fetch(request)
            self.egg_storage.put(response.buffer, self.task.project_name, self.task.project_version)
            logger.debug('download egg done.')
            requirements = workspace.find_project_requirements(self.task.project_name, egg_storage=self.egg_storage)
            yield workspace.pip_install(requirements)
            result = yield self.execute_subprocess()
        except ProcessFailed as e:
            logger.error(e)
            error_log = e.message
            if e.std_output:
                logger.error(e.std_output)
                error_log += e.std_output
            result = self.complete_with_error(error_log)
        except Exception as e:
            logger.error(e)
            error_log = e.message
            result = self.complete_with_error(error_log)
        raise gen.Return(result)

    def check_process(self):
        execute_result = self.p.poll()
        logger.debug('check process')
        if execute_result is not None:
            logger.info('task complete')
            self.complete(execute_result)

    def execute_subprocess(self):
        future = Future()
        # init items file
        workspace = ProjectWorkspace(self.task.project_name)
        self.items_file = os.path.join(self.workspace_dir, '%s.%s' % (self.task.id, 'jl'))
        python = workspace.python
        runner = 'scrapyd.runner'
        pargs = [python, '-m', runner, 'crawl', self.task.spider_name]

        env = os.environ.copy()
        env['SCRAPY_PROJECT'] = str(self.task.project_name)
        env['SCRAPY_JOB'] = str(self.task.id)
        env['SCRAPY_FEED_URI'] = str(path_to_file_uri(self.items_file))
        try:
            self.p = subprocess.Popen(pargs, env=env, stdout=self._f_output, cwd=self.workspace_dir, stderr=self._f_output)
            if self.on_subprocess_start:
                self.on_subprocess_start(self.task, self.p.pid)

        except Exception as e:
            return self.complete_with_error('Error when starting crawl subprocess : %s' % e)
        logger.info('job %s started on pid: %d' % (self.task.id, self.p.pid))

        def check_process():
            execute_result = self.p.poll()
            logger.debug('check process')
            if execute_result is not None:
                logger.info('task complete')
                future.set_result(self.complete(execute_result))

        self.check_process_callback = PeriodicCallback(check_process, 1*1000)
        self.check_process_callback.start()
        return future

    def result(self):
        return self

    def complete(self, ret_code):
        self._f_output.close()
        self.ret_code = ret_code
        self.check_process_callback.stop()
        self.check_process_callback = None
        return self.result()

    def complete_with_error(self, error_message):
        logger.error(error_message)
        self._f_output.write(error_message)
        self._f_output.close()
        self.ret_code = 1
        return self.result()

    def __del__(self):
        logger.debug('delete task executor for task %s' % self.task.id)
        if self.workspace_dir and os.path.exists(self.workspace_dir):
            shutil.rmtree(self.workspace_dir)



