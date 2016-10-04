from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.queues import Queue
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


class Executor():
    heartbeat_interval = 10
    checktask_interval = 10
    logger = logging.getLogger(__name__)
    task_queue = Queue()

    def __init__(self):
        self.service_base = 'http://localhost:8888'

    def start(self):
        self.register_node()
        ioloop = IOLoop.current()
        heartbeat_callback = PeriodicCallback(self.send_heartbeat, self.heartbeat_interval*1000, ioloop)
        heartbeat_callback.start()

        checktask_callback = PeriodicCallback(self.check_task, self.checktask_interval*1000, ioloop )
        checktask_callback.start()
        #ioloop.call_later(delay=1, callback=poll, node_id = self.node_id)

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
        response_data = json.loads(res.read())
        print response_data
        if response_data['data'] is not None:
            task = SpiderTask()
            task.id = response_data['data']['task']['task_id']
            task.spider_id = response_data['data']['task']['spider_id']
            task.project_name = response_data['data']['task']['project_name']
            task.project_version = response_data['data']['task']['version']
            task.spider_name = response_data['data']['task']['spider_name']
            return task


    def check_task(self):
        if self.task_queue.empty():
            task = self.get_next_task()
            if task is not None:
                self.task_queue.put(task)

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
    logging.basicConfig(level=logging.INFO)
    executor = Executor()
    executor.start()
