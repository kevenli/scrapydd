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


class SpiderTask():
    id = None
    spider_id = None
    project_name = None
    spider_name = None
    project_version = None
    executor = None


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
        ioloop = IOLoop.current()
        #init heartbeat period callback
        heartbeat_callback = PeriodicCallback(self.send_heartbeat, self.heartbeat_interval*1000)
        heartbeat_callback.start()

        #init checktask period callback
        checktask_callback = PeriodicCallback(self.check_task, self.checktask_interval*1000)
        checktask_callback.start()

        ioloop.start()

    def send_heartbeat(self):
        url = urlparse.urljoin(self.service_base, '/nodes/%d/heartbeat' % self.node_id)
        request = urllib2.Request(url, data='')
        try:
            res = urllib2.urlopen(request)
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
                self.task_finished()
            egg = StringIO(res.read())

            egg_storage.put(egg, task.project_name, task.project_version)

        self.test_egg_requirements(task.project_name)

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


    def task_finished(self, future):
        task = self.task_queue.get_nowait()
        try:
            url = urlparse.urljoin(self.service_base, '/executing/complete')
            post_data = urllib.urlencode({'task_id': task.id, 'log': task.executor.p.stdout.read()})
            request = urllib2.Request(url, post_data)
            urllib2.urlopen(request)
            logging.info('task %s finished' % task.id)
        except urllib2.URLError:
            logging.warning('Cannot connect to server.')


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
        runner = 'scrapyd.runner'
        pargs = [sys.executable, '-m', runner, 'crawl', self.task.spider_name]
        #pargs = [sys.executable, '-m', runner, 'list']
        env = os.environ.copy()
        logging.debug(env)
        env['SCRAPY_PROJECT'] = str(self.task.project_name)
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
