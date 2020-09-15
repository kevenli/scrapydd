"""
This module is used by agent to execute spider task.
"""
# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
import asyncio
import os
import logging
import tempfile
import shutil
from urllib.parse import urlparse
from configparser import ConfigParser
from threading import Thread
from tornado.ioloop import IOLoop, PeriodicCallback
from .config import AgentConfig
from .workspace import RunnerFactory, DictSpiderSettings, CrawlResult
from .exceptions import ProcessFailed
from .client import get_client, NoJobAvailable

logger = logging.getLogger(__name__)

TASK_STATUS_SUCCESS = 'success'
TASK_STATUS_FAIL = 'fail'

EXECUTOR_STATUS_OFFLINE = 0
EXECUTOR_STATUS_ONLINE = 1


class SpiderTask:
    """
    Spider task description data fetched from server.
    """
    id = None
    spider_id = None
    project_name = None
    spider_name = None
    project_version = None
    spider_parameters = None
    extra_requirements = None
    settings = None
    figure = None


class TaskSlotContainer:
    """
    Slot container to control concurrency running on agent side.
    Agent need allocate a slot before running.
    """
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
            raise ValueError('Task in TaskSlotContainer must be '
                             'TaskExecutor type.')
        for i in range(len(self.slots)):
            if self.slots[i] is None:
                self.slots[i] = task
                break

    def remove_task(self, task):
        for i in range(len(self.slots)):
            if self.slots[i] == task:
                self.slots[i] = None
                break

    def tasks(self):
        for item in self.slots:
            if item is not None:
                yield item

    def get_task(self, task_id):
        for item in self.slots:
            if item and item.task.id == task_id:
                return item
        return None

    @property
    def max_size(self):
        return self._max_size


class Executor:
    """
    Main object communicate to server and control tasks running.
    """
    # hearbeat check interval, default: 10 seconds
    heartbeat_interval = 10 * 1000
    # check task interval, default: 10 seconds
    checktask_interval = 10 * 1000
    custom_ssl_cert = False

    def __init__(self, config=None):

        self.ioloop = IOLoop.current()
        self.node_id = None
        self.status = EXECUTOR_STATUS_OFFLINE
        if config is None:
            config = AgentConfig()
        self.config = config
        self.tags = config.get('tags')
        self.checktask_callback = None

        self.task_slots = TaskSlotContainer(config.getint('slots', 1))

        server_base = config.get('server')
        if urlparse(server_base).scheme == '':
            if config.getint('server_https_port'):
                server_https_port = config.getint('server_https_port')
                server_base = 'https://%s:%d' % (server_base,
                                                 server_https_port)
            else:
                server_base = 'http://%s:%d' % (server_base,
                                                config.getint('server_port'))
        self.service_base = server_base
        self.keep_job_files = config.getboolean('debug', False)
        logger.debug('keep_job_files %s', self.keep_job_files)

        node_key = None
        secret_key = None
        node_id = None
        if os.path.exists('conf/node.conf'):
            parser = ConfigParser()
            parser.read('conf/node.conf')
            node_key = parser.get('agent', 'node_key')
            secret_key = parser.get('agent', 'secret_key')
            node_id = int(parser.get('agent', 'node_id'))
        self.client = get_client(config,
                                 app_key=node_key,
                                 app_secret=secret_key,
                                 node_id=node_id)
        self.runner_factory = RunnerFactory(config)

    def start(self):
        self.register_node()
        # init heartbeat period callback
        heartbeat_callback = PeriodicCallback(self.send_heartbeat,
                                              self.heartbeat_interval)
        heartbeat_callback.start()

        self.ioloop.start()

    def send_heartbeat(self):
        if self.status == EXECUTOR_STATUS_OFFLINE:
            self.register_node()
            return
        running_job_ids = [task_executor.task.id for task_executor in
                                   self.task_slots.tasks()]
        res = self.client.heartbeat(running_job_ids=running_job_ids)
        for job_id in res['kill_job_ids'] or []:
            task_to_kill = self.task_slots.get_task(job_id)
            if task_to_kill:
                logger.info('%s', task_to_kill)
                task_to_kill.kill()
                self.task_slots.remove_task(task_to_kill)

        if res['new_job_available']:
            self.ioloop.call_later(0, self.check_task)

    def register_node(self):
        self.client.login()
        self.status = EXECUTOR_STATUS_ONLINE
        self.node_id = self.client._node_id

    def on_new_task_reach(self, job):
        package = job.package
        if not package:
            package = self.client.get_job_egg(job.id)
        executor = TaskExecutor(job, package, self.runner_factory)
        self.task_slots.put_task(executor)
        thread = Thread(target=self.run_job, args=[executor])
        thread.start()

    def get_next_task(self):
        try:
            task = self.client.get_next_job()
            self.on_new_task_reach(task)
        except NoJobAvailable as ex:
            logger.warn('NoJobAvailable')
        except Exception as ex:
            logger.error(ex)

    def execute_task(self, task):
        package_egg = self.client.get_job_egg(task.id)
        executor = TaskExecutor(task, egg_downloader=package_egg,
                                runner_factory=self.runner_factory,
                                keep_files=self.keep_job_files)
        future = executor.execute()
        self.ioloop.add_future(future, self.task_finished)
        return executor

    def run_job(self, executor):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        logger.info('start run job %s', executor.task.id)
        result = loop.run_until_complete(executor.execute())
        try:
            self.complete_task(executor, result)
        except Exception as ex:
            logger.error(ex)
            self.task_slots.remove_task(executor)

    def complete_task(self, task_executor, result):
        """
        @type task_executor: TaskExecutor
        """
        status = 'success' if result.ret_code == 0 else 'fail'
        self.client.complete_job(task_executor.task.id, status,
                                 items_file=result.items_file,
                                 logs_file=result.crawl_logfile)
        self.task_slots.remove_task(task_executor)

    def task_finished(self, future):
        task_executor = future.result()
        status = TASK_STATUS_SUCCESS if task_executor.ret_code == 0 \
            else TASK_STATUS_FAIL
        self.complete_task(task_executor, status)

    def check_task(self):
        if not self.task_slots.is_full():
            self.get_next_task()


class TaskExecutor:
    def __init__(self, task, egg_downloader, runner_factory, keep_files=False):
        """
        @type task: SpiderTask
        @type egg_downloader: ProjectEggDownloader
        @type runner_factory: RunnerFactory
        """
        self.task = task
        self.egg_downloader = egg_downloader
        self._runner_factory = runner_factory
        self._spider_settings = DictSpiderSettings(task.figure or
                                                   task.settings['task'])
        self._f_output = None
        self.output_file = None
        self.items_file = None
        self.ret_code = None
        prefix = 'ddjob-%s-%s-' % (task.project_name, task.id)
        self.workspace_dir = tempfile.mkdtemp(prefix=prefix)
        if not os.path.exists(self.workspace_dir):
            os.makedirs(self.workspace_dir)
        self.output_file = str(os.path.join(self.workspace_dir, 'crawl.log'))
        self._f_output = open(self.output_file, 'w')
        self.on_subprocess_start = None
        self.keep_files = keep_files
        self._runner = None

    async def execute(self):
        try:
            logger.info('start fetch spider egg.')
            runner = self._runner_factory.build(self.egg_downloader)
            self._runner = runner
            crawl_result = await runner.crawl(self._spider_settings)
            self.items_file = crawl_result.items_file
            self.output_file = crawl_result.crawl_logfile
            return crawl_result
        except ProcessFailed as ex:
            logger.warning('Process Failed when executing task %s: %s',
                           self.task.id, ex)
            error_log = ex.message
            if ex.std_output:
                logger.warning(ex.std_output)
                error_log += ex.std_output
            if ex.err_output:
                logger.warning(ex.err_output)
                error_log += ex.err_output
            self.complete_with_error(error_log)
            crawl_result = CrawlResult(1, runner=self)
            crawl_result.crawl_logfile = self.output_file
            return crawl_result
        except Exception as ex:
            logger.error('Error when executing task %s: %s', self.task.id, ex)
            error_log = str(ex)
            result = self.complete_with_error(error_log)
            crawl_result = CrawlResult(1, runner=self)
            crawl_result.crawl_logfile = self.output_file
            return crawl_result

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
        self._f_output.close()
        if not self.keep_files:
            logger.debug('delete task executor for task %s', self.task.id)
            if self.workspace_dir and os.path.exists(self.workspace_dir):
                shutil.rmtree(self.workspace_dir)
            if self._runner:
                self._runner.clear()
                self._runner = None

    def kill(self):
        if not self._f_output.closed:
            self._f_output.write("Received a kill command, stopping spider.")
        self._runner.kill()
