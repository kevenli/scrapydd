from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.queues import Queue
from tornado.concurrent import Future
import urllib2, urllib
import json
from scrapyd.eggstorage import FilesystemEggStorage
import scrapyd.config
import subprocess
import sys
import os
import urlparse
import logging
import tempfile
import shutil
import pkg_resources
import time
from config import AgentConfig
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from stream import MultipartRequestBodyProducer
from w3lib.url import path_to_file_uri
import socket
from tornado import gen
from workspace import ProjectWorkspace

logger = logging.getLogger(__name__)

TASK_STATUS_SUCCESS = 'success'
TASK_STATUS_FAIL = 'fail'
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

class Executor():
    heartbeat_interval = 10
    checktask_interval = 10
    task_queue = Queue()

    def __init__(self, config=None):
        self.ioloop = IOLoop.current()
        self.node_id = None

        if config is None:
            config =AgentConfig()
        self.task_slots = TaskSlotContainer(config.getint('slots', 1))
        self.config = config
        self.service_base = 'http://%s:%d' % (config.get('server'), config.getint('server_port'))

    def start(self):
        logger.info('------------------------')
        logger.info('Starting scrapydd agent.')
        logger.info('------------------------')
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


    def send_heartbeat(self):
        url = urlparse.urljoin(self.service_base, '/nodes/%d/heartbeat' % self.node_id)
        running_tasks = ','.join([task.id for task in self.task_slots.tasks()])
        request = urllib2.Request(url, data='', headers={'X-DD-RunningJobs': running_tasks})
        try:
            res = urllib2.urlopen(request)
            self.check_header_new_task_on_server(res.info())
            response_data = res.read()
        except urllib2.HTTPError as e:
            if e.code == 400:
                logging.warning('Node expired, register now.')
                self.register_node()
        except urllib2.URLError:
            logging.warning('Cannot connect to server.')


    def register_node(self):
        while True:
            try:
                url = urlparse.urljoin(self.service_base, '/nodes')
                request = urllib2.Request(url, data='')
                res = urllib2.urlopen(request)
                self.node_id = json.loads(res.read())['id']
                logger.info('node %d registered' % self.node_id)
                return
            except urllib2.URLError:
                logging.warning('Cannot connect to server')
                time.sleep(10)
            except socket.error:
                logging.warning('Cannot connect to server')
                time.sleep(10)


    def get_next_task(self):
        url = urlparse.urljoin(self.service_base, '/executing/next_task')
        post_data = urllib.urlencode({'node_id': self.node_id})
        request = urllib2.Request(url=url, data=post_data)
        try:
            res = urllib2.urlopen(request)
            response_content = res.read()
            response_data = json.loads(response_content)
            logging.debug(url)
            logging.debug(response_content)
            if response_data['data'] is not None:
                task = SpiderTask()
                task.id = response_data['data']['task']['task_id']
                task.spider_id = response_data['data']['task']['spider_id']
                task.project_name = response_data['data']['task']['project_name']
                task.project_version = response_data['data']['task']['version']
                task.spider_name = response_data['data']['task']['spider_name']
                return task
        except urllib2.URLError:
            logger.warning('Cannot connect to server')

    def execute_task(self, task):
        executor = TaskExecutor(task, config=self.config)
        future, pid = executor.begin_execute()
        self.post_start_task(task, pid)
        self.ioloop.add_future(future, self.task_finished)

    def post_start_task(self, task, pid):
        url = urlparse.urljoin(self.service_base, '/jobs/%s/start' % task.id)
        post_data = urllib.urlencode({'pid':pid})
        try:
            request = urllib2.Request(url, post_data)
            urllib2.urlopen(request)
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
        request = HTTPRequest(url, method='POST', headers=headers, body_producer=MultipartRequestBodyProducer(datagen))
        client = AsyncHTTPClient()
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
            task_executor.clear()
            self.task_slots.remove_task(task_executor.task)
            logger.debug('complete_task_done')
        return complete_task_done_f

    def task_finished(self, future):
        task_executor = future.result()
        self.complete_task(task_executor, TASK_STATUS_SUCCESS if task_executor.ret_code == 0 else TASK_STATUS_FAIL)

    def check_task(self):
        if not self.task_slots.is_full():
            task = self.get_next_task()
            if task is not None:
                self.task_slots.put_task(task)
                self.execute_task(task)


class TaskExecutor():
    def __init__(self, task, config=None):
        '''
        @type task: SpiderTask
        '''
        self.task = task
        if config is None:
            config = AgentConfig()
        self.service_base = 'http://%s:%d' % (config.get('server'), config.getint('server_port'))
        self._f_output = None
        self.output_file = None
        self.future = Future()
        self.p = None
        self.check_process_callback = None
        self.items_file = None
        self.ret_code = None
        self.workspace_dir = os.path.abspath(os.path.join('workspace', self.task.project_name))
        if not os.path.exists(self.workspace_dir):
            os.makedirs(self.workspace_dir)
        self.output_file = str(os.path.join(self.workspace_dir, '%s.log' % self.task.id))
        self._f_output = open(self.output_file, 'w')
        self.p_test_egg_requirements = None
        self.p_test_egg_requirements_callback = PeriodicCallback(self.test_egg_requirements_check, 1*1000)
        self.check_process_callback = PeriodicCallback(self.check_process, 1000)

        eggs_dir = os.path.join(self.workspace_dir, 'eggs')
        if not os.path.exists(eggs_dir):
            os.mkdir(eggs_dir)
        self.egg_storage = FilesystemEggStorage(scrapyd.config.Config(values={'eggs_dir': eggs_dir}))

    def begin_execute(self):
        self.download_egg()
        return self.future, None

    @gen.coroutine
    def execute(self):
        workspace = ProjectWorkspace(self.task.project_name)
        logger.debug('begin download egg.')
        egg_request_url = urlparse.urljoin(self.service_base, '/spiders/%d/egg' % self.task.spider_id)
        request = HTTPRequest(egg_request_url)
        client = AsyncHTTPClient()
        response = yield client.fetch(request, callback=self.download_egg_done)
        self.egg_storage.put(response.buffer, self.task.project_name, self.task.project_version)
        logger.debug('download egg done.')
        requirements = workspace.find_project_requirements()

        yield workspace.pip_install(requirements)
        self.execute_subprocess()

    def check_process(self):
        execute_result = self.p.poll()
        logger.debug('check process')
        if execute_result is not None:
            logger.info('task complete')
            self.complete(execute_result)

    def download_egg(self):
        logger.debug('begin download egg.')
        egg_request_url = urlparse.urljoin(self.service_base, '/spiders/%d/egg' % self.task.spider_id)
        request = HTTPRequest(egg_request_url)
        client = AsyncHTTPClient()
        client.fetch(request, callback=self.download_egg_done)

    def download_egg_done(self, response):
        '''
        :type response: tornado.webclient.HTTPResponse
        :return:
        '''
        if response.error:
            return self.complete_with_error("Error when retrieving egg : %s" % response.error)

        self.egg_storage.put(response.buffer, self.task.project_name, self.task.project_version)
        logger.debug('download egg done.')
        self.test_egg_requirements(self.task.project_name)

    def execute_subprocess(self):
        # init items file
        self.items_file = os.path.join(self.workspace_dir, '%s.%s' % (self.task.id, 'jl'))
        python = os.path.join(self.workspace_dir, 'bin', 'python')
        runner = 'scrapyd.runner'
        pargs = [python, '-m', runner, 'crawl', self.task.spider_name]

        env = os.environ.copy()
        env['SCRAPY_PROJECT'] = str(self.task.project_name)
        env['SCRAPY_JOB'] = str(self.task.id)
        env['SCRAPY_FEED_URI'] = str(path_to_file_uri(self.items_file))
        try:
            self.p = subprocess.Popen(pargs, env=env, stdout=self._f_output, cwd=self.workspace_dir, stderr=self._f_output)
        except Exception as e:
            return self.complete_with_error('Error when starting crawl subprocess : %s' % e)
        logger.info('job %s started on pid: %d' % (self.task.id, self.p.pid))

        self.check_process_callback.start()

    def test_egg_requirements(self, project):
        logger.debug('enter test_egg')
        workspace = ProjectWorkspace(project)
        try:
            requirements = workspace.find_project_requirements(project, egg_storage=self.egg_storage)
            requirements += ['scrapyd']
            workspace.init().result()
            logger.debug('Begin test requirements.')

            def callback(future):
                self.execute_subprocess()
            workspace.pip_install(requirements).add_done_callback(callback)
        except Exception as e:
            self.complete_with_error('Error when creating virtualenv: %s' % e)

    def test_egg_requirements_check(self):
        ret_code = self.p_test_egg_requirements.poll()
        logger.debug('check egg requirements process')
        if ret_code is not None:
            self.p_test_egg_requirements_callback.stop()
            if ret_code == 0:
                logger.info('requirements satisfied.')
                self.execute_subprocess()
            else:
                self.complete_with_error('Error when install requirements.')

    def complete(self, ret_code):
        self._f_output.close()
        self.ret_code = ret_code
        self.check_process_callback.stop()
        self.future.set_result(self)

    def complete_with_error(self, error_message):
        logger.error(error_message)
        self._f_output.write(error_message)
        self._f_output.close()
        self.ret_code = 1
        self.future.set_result(self)

    def clear(self):
        if self.items_file and os.path.exists(self.items_file):
            os.remove(self.items_file)
        if self.output_file and os.path.exists(self.output_file):
            os.remove(self.output_file)


