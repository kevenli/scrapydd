import unittest
from scrapydd.agent import AgentConfig
from scrapydd.executor import SpiderTask, TaskExecutor
from tornado.concurrent import Future
from tornado.ioloop import IOLoop, PeriodicCallback
import logging

@unittest.skip
class TaskExecutorTests(unittest.TestCase):
    def test_execute(self):
        task =SpiderTask()
        task.spider_name
        config = AgentConfig()


class FakeEggDownloader(object):
    def __init__(self, egg_filepath):
        self.egg_filepath = egg_filepath

    def download_egg_future(self, spider_id):
        ret_future = Future()
        ret_future.set_result(self.egg_filepath)
        return ret_future

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    task = SpiderTask()
    task.spider_id = 1
    task.project_name = 'test_project'
    task.spider_name = 'success_spider'
    task.project_version = '1.0'
    task.id = 1
    task.spider_parameters = {}

    def execute(future=None):
        executor = TaskExecutor(task, FakeEggDownloader('test_project/dist/test_project-1.0-py2.7.egg'), '/work')
        executor.execute().add_done_callback(execute)
    #period = PeriodicCallback(execute, 1000)
    #period.start()
    execute()
    IOLoop.current().start()