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

egg_storage = FilesystemEggStorage(scrapyd.config.Config())

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
    executor = None
    ret_code = None
    items_file = None


class Executor():
    heartbeat_interval = 10
    checktask_interval = 10
    logger = logging.getLogger('scrapymill.executor')
    task_queue = Queue()

    def __init__(self, config=None):
        self.ioloop = IOLoop.current()
        #self.service_base = 'http://localhost:6800'
        if config is None:
            config =AgentConfig()
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
        request = urllib2.Request(url, data='')
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
                self.logger.info('node %d registered' % self.node_id)
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
            logging.warning('Cannot connect to server')

    def test_egg_requirements(self, project):
        logging.debug('enter test_egg')
        version, eggfile = egg_storage.get(project)
        logging.debug(version)
        if eggfile:
            prefix = '%s-%s-' % (project, version)
            fd, eggpath = tempfile.mkstemp(prefix=prefix, suffix='.egg')

            lf = os.fdopen(fd, 'wb')
            shutil.copyfileobj(eggfile, lf)
            lf.close()
        logging.debug(eggpath)
        try:
            d = pkg_resources.find_distributions(eggpath).next()
        except StopIteration:
            raise ValueError("Unknown or corrupt egg")
        else:
            eggpath = None
        logging.debug(d.requires())
        for require in d.requires():
            subprocess.check_call(['pip', 'install', str(require)])

        try:
            assert 'scrapy.conf' not in sys.modules, "Scrapy settings already loaded"
        finally:
            if eggpath:
                os.remove(eggpath)

    def execute_task(self, task):
        logging.debug(task.project_version)
        logging.debug(egg_storage.list(task.project_name))
        if not task.project_version in egg_storage.list(task.project_name):
            egg_request_url = urlparse.urljoin(self.service_base, '/spiders/%d/egg' % task.spider_id)
            logging.debug(egg_request_url)
            egg_request=urllib2.Request(egg_request_url)
            try:
                res = urllib2.urlopen(egg_request)
            except urllib2.URLError:
                logging.warning("Cannot retrieve job's egg, removing job")
                task = self.task_queue.get_nowait()  # pop the failed task from queue
                self.complete_task(task, TASK_STATUS_FAIL, "Cannot retrieve job's egg, removing job")
                return
            egg = StringIO(res.read())

            egg_storage.put(egg, task.project_name, task.project_version)

        try:
            self.test_egg_requirements(task.project_name)
        except ValueError as e:
            task = self.task_queue.get_nowait()    # pop the failed task from queue
            self.complete_task(task, TASK_STATUS_FAIL, e.message)
            return

        executor = TaskExecutor(task)
        task.executor = executor
        future, pid = executor.begin_execute()
        self.ioloop.add_future(future, self.task_finished)
        self.post_start_task(task, pid)

    def post_start_task(self, task, pid):
        url = urlparse.urljoin(self.service_base, '/jobs/%s/start' % task.id)
        post_data = urllib.urlencode({'pid':pid})
        request = urllib2.Request(url, post_data)
        urllib2.urlopen(request)

    def complete_task(self, task, status, log):
        try:
            url = urlparse.urljoin(self.service_base, '/executing/complete')
            post_data = {
                'task_id': task.id,
                'log': log,
                'status':status,
            }
            if task.items_file and os.path.exists(task.items_file):
                post_data['items'] = open(task.items_file, "rb")
            datagen, headers = multipart_encode(post_data)
            request = urllib2.Request(url, datagen, headers)
            urllib2.urlopen(request)
            logging.info('task %s finished' % task.id)
        except urllib2.URLError:
            logging.warning('Cannot connect to server.')


    def task_finished(self, future):
        task = self.task_queue.get_nowait()
        self.complete_task(task, 'success' if task.ret_code == 0 else 'fail', task.executor.p.stdout.read())

    def check_task(self):
        if self.task_queue.empty():
            task = self.get_next_task()
            if task is not None:
                self.task_queue.put(task)
                self.execute_task(task)


class TaskExecutor():
    def __init__(self, task):
        self.task = task

    def begin_execute(self):
        from w3lib.url import path_to_file_uri
        runner = 'scrapyd.runner'
        pargs = [sys.executable, '-m', runner, 'crawl', self.task.spider_name]
        #pargs = [sys.executable, '-m', runner, 'list']
        env = os.environ.copy()
        logging.debug(env)
        env['SCRAPY_PROJECT'] = str(self.task.project_name)
        env['SCRAPY_JOB'] = self.task.id
        self.task.items_file = os.path.join('items', self.task.project_name, self.task.spider_name, '%s.%s' % (self.task.id, 'jl'))
        env['SCRAPY_FEED_URI'] = path_to_file_uri(self.task.items_file)

        self.p = subprocess.Popen(pargs, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        logging.info('job started %d' % self.p.pid)
        self.future = Future()
        self.check_process_callback = PeriodicCallback(self.check_process, 1000)
        self.check_process_callback.start()
        return self.future, self.p.pid

    def check_process(self):
        execute_result = self.p.poll()
        logging.debug('check process')
        if execute_result is not None:
            logging.info('task complete')
            self.task.ret_code = execute_result
            self.check_process_callback.stop()
            self.future.set_result(self.task)


def run(argv=None):
    config = AgentConfig()
    if config.getboolean('debug'):
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    if argv is None:
        argv = sys.argv
    executor = Executor(config)
    executor.start()

if __name__ == '__main__':
    run()
