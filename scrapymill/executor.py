from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.queues import Queue
from tornado.concurrent import Future
import urllib2, urllib
import json
from scrapyd.eggstorage import FilesystemEggStorage
from scrapyd.config import Config
from StringIO import StringIO
import subprocess
import sys
import os
import urlparse
import logging

egg_storage = FilesystemEggStorage(Config())


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

    def __init__(self):
        self.ioloop = IOLoop.current()
        self.service_base = 'http://localhost:8888'

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
        res = urllib2.urlopen(request)
        res.read()

    def register_node(self):
        url = urlparse.urljoin(self.service_base, '/nodes')
        request = urllib2.Request(url, data='')
        res = urllib2.urlopen(request)
        self.node_id = json.loads(res.read())['id']
        self.logger.info('node %d registered' % self.node_id)


    def get_next_task(self):
        url = 'http://localhost:8888/executing/next_task'
        post_data = urllib.urlencode({'node_id': self.node_id})
        request = urllib2.Request(url=url, data=post_data)
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

    def execute_task(self, task):
        logging.debug(task.project_version)
        logging.debug(egg_storage.list(task.project_name))
        if not task.project_version in egg_storage.list(task.project_name):
            egg_request_url = urlparse.urljoin(self.service_base, '/spiders/%d/egg' % task.spider_id)
            logging.debug(egg_request_url)
            egg_request=urllib2.Request(egg_request_url)
            res = urllib2.urlopen(egg_request)
            egg = StringIO(res.read())

            egg_storage.put(egg, task.project_name, task.project_version)

        executor = TaskExecutor(task)
        task.executor = executor
        future = executor.begin_execute()
        self.ioloop.add_future(future, self.task_finished)

    def task_finished(self, future):
        task = self.task_queue.get_nowait()
        print task
        url = urlparse.urljoin(self.service_base, '/executing/complete')
        post_data = urllib.urlencode({'task_id': task.id, 'log': task.executor.p.stdout.read()})
        request = urllib2.Request(url, post_data)
        urllib2.urlopen(request)
        print 'task finished'

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
        self.future = Future()
        self.check_process_callback = PeriodicCallback(self.check_process, 1000)
        self.check_process_callback.start()
        return self.future

    def check_process(self):
        execute_result = self.p.poll()
        if execute_result is not None:
            self.check_process_callback.stop()
            self.future.set_result(self.task)

def get_next_task(node_id):
    url = 'http://localhost:8888/executing/next_task'
    post_data = urllib.urlencode({'node_id':node_id})
    request = urllib2.Request(url=url, data=post_data)
    res = urllib2.urlopen(request)
    response_data = json.loads(res.read())
    return response_data

def poll(node_id):
    response_data = get_next_task(node_id)
    if 'spider_id' in response_data:
        task_id = response_data['task_id']
        spider_id = response_data['spider_id']
        project_name = response_data['project_name']
        spider_name = response_data['spider_name']
        version = response_data['version']

        if not version in egg_storage.list(project_name):
            egg_request_url = 'http://localhost:8888/spiders/%d/egg' % spider_id
            print egg_request_url
            egg_request=urllib2.Request(egg_request_url)
            res = urllib2.urlopen(egg_request)
            egg = StringIO(res.read())

            egg_storage.put(egg, project_name, version)

        runner = 'scrapyd.runner'
        pargs = [sys.executable, '-m', runner, 'crawl', spider_name]
        env = os.environ.copy()
        env['SCRAPY_PROJECT'] = str(project_name)
        subprocess.call(pargs, env=env)

        task_complete_url = 'http://localhost:8888/executing/complete'
        task_complete_postdata = urllib.urlencode({'task_id':task_id})
        task_complete_request = urllib2.Request(task_complete_url, data=task_complete_postdata)
        urllib2.urlopen(task_complete_request).read()

    ioloop = IOLoop.current()
    ioloop.call_later(1, poll)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    executor = Executor()
    executor.start()
