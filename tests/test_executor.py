import unittest
from scrapydd.agent import AgentConfig
from scrapydd.executor import SpiderTask


@unittest.skip
class TaskExecutorTests(unittest.TestCase):
    def test_execute(self):
        task =SpiderTask()
        task.spider_name
        config = AgentConfig()


