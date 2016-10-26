from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.queues import Queue
from tornado.concurrent import Future
import urllib2, urllib
import json
from scrapyd.eggstorage import FilesystemEggStorage
import scrapyd.config
from StringIO import StringIO
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

egg_storage = FilesystemEggStorage(scrapyd.config.Config())

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
        executor = TaskExecutor(task)
        future, pid = executor.begin_execute()
        self.post_start_task(task, pid)
        self.ioloop.add_future(future, self.task_finished)

    def post_start_task(self, task, pid):
        url = urlparse.urljoin(self.service_base, '/jobs/%s/start' % task.id)
        post_data = urllib.urlencode({'pid':pid})
        request = urllib2.Request(url, post_data)
        urllib2.urlopen(request)

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
    def __init__(self, task):
        '''
        @type task: SpiderTask
        '''
        self.task = task
        config = AgentConfig()
        self.service_base = 'http://%s:%d' % (config.get('server'), config.getint('server_port'))
        self._f_output = None
        self.output_file = None
        self.future = Future()
        self.p = None
        self.check_process_callback = None
        self.items_file = None
        self.ret_code = None

    def begin_execute(self):
        # init log file
        workspace_dir = os.path.join('workspace', self.task.project_name, self.task.spider_name)
        if not os.path.exists(workspace_dir):
            os.makedirs(workspace_dir)
        self.output_file = str(os.path.join(workspace_dir, '%s.log' % self.task.id))
        self._f_output = open(self.output_file, 'w')

        # try download
        try:
            self.download_egg()
        except Exception as e:
            logger.error('Error when downloading egg file : %s' % e)
            return self.complete_with_error(e.message)

        # try install requirements
        try:
            self.test_egg_requirements(self.task.project_name)
        except Exception as e:
            logger.error('Error when test egg requirements: %s' % e)
            return self.complete_with_error(e.message)

        # init items file
        self.items_file = os.path.join(workspace_dir, '%s.%s' % (self.task.id, 'jl'))

        runner = 'scrapyd.runner'
        pargs = [sys.executable, '-m', runner, 'crawl', self.task.spider_name]

        env = os.environ.copy()
        env['SCRAPY_PROJECT'] = str(self.task.project_name)
        env['SCRAPY_JOB'] = str(self.task.id)
        env['SCRAPY_FEED_URI'] = str(path_to_file_uri(self.items_file))
        self.p = subprocess.Popen(pargs, env=env, stdout=self._f_output, stderr=self._f_output)
        logger.info('job %s started on pid: %d' % (self.task.id, self.p.pid))
        self.future = Future()
        self.check_process_callback = PeriodicCallback(self.check_process, 1000)
        self.check_process_callback.start()
        return self.future, self.p.pid

    def check_process(self):
        execute_result = self.p.poll()
        logger.debug('check process')
        if execute_result is not None:
            logger.info('task complete')
            self.complete(execute_result)


    def download_egg(self):
        if not self.task.project_version in egg_storage.list(self.task.project_name):
            egg_request_url = urlparse.urljoin(self.service_base, '/spiders/%d/egg' % self.task.spider_id)
            logger.debug(egg_request_url)
            egg_request=urllib2.Request(egg_request_url)
            try:
                res = urllib2.urlopen(egg_request)
            except urllib2.URLError:
                logger.warning("Cannot retrieve job's egg, removing job")
                raise Exception("Cannot retrieve job's egg, removing job")
            egg = StringIO(res.read())
            egg_storage.put(egg, self.task.project_name, self.task.project_version)


    def test_egg_requirements(self, project):
        logger.debug('enter test_egg')
        version, eggfile = egg_storage.get(project)
        logger.debug(version)
        if eggfile:
            prefix = '%s-%s-' % (project, version)
            fd, eggpath = tempfile.mkstemp(prefix=prefix, suffix='.egg')

            lf = os.fdopen(fd, 'wb')
            shutil.copyfileobj(eggfile, lf)
            lf.close()
            logger.debug(eggpath)
        try:
            d = pkg_resources.find_distributions(eggpath).next()
        except StopIteration:
            raise ValueError("Unknown or corrupt egg")
            logger.debug(d.requires())
        for require in d.requires():
            subprocess.check_call(['pip', 'install', str(require)], stderr=self._f_output)
        if eggpath:
            logger.debug('removing: ' + eggpath)
            os.remove(eggpath)

    def complete(self, ret_code):
        self._f_output.close()
        self.ret_code = ret_code
        self.check_process_callback.stop()
        self.future.set_result(self)

    def complete_with_error(self, error_message):
        self._f_output.write(error_message)
        self._f_output.close()
        self.ret_code = 1
        self.future.set_result(self)
        return self.future, None

    def clear(self):
        if self.items_file and os.path.exists(self.items_file):
            os.remove(self.items_file)
        if self.output_file and os.path.exists(self.output_file):
            os.remove(self.output_file)

def init_logging(config):
    import logging.handlers
    logger = logging.getLogger()
    fh = logging.handlers.TimedRotatingFileHandler('scrapydd-agent.log', when='D', backupCount=7)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    if config.getboolean('debug'):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


def run(argv=None):
    config = AgentConfig()
    init_logging(config)

    if argv is None:
        argv = sys.argv
    executor = Executor(config)
    logger.info('------------------------')
    logger.info('Starting scrapydd agent.')
    logger.info('------------------------')
    executor.start()

if __name__ == '__main__':
    run()
