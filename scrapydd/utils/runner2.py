"""
    This runner is designed to run subprocess commands in a container environment.
    To archive this, caller should prepare all necessary files placed in some mapped
    volume a container can access (namely Working Folder), a sequence processes should
    be triggered in one shot.

    This runner can be used by the server what to verify a package and retrieve SPIDERS
    of the package whenever a new package is uploaded. Moreover an agent can run a
    crawl job in a container. Either of these scenarios which need run a custom package
    should consider about the security risks, which may be unauthorized file operations
    or creating unexpected processes.

    To avoid unauthorized file operations, the running container should only map to
    the working folder which contains only necessary files of a job.

    To avoid creating unexpected processes, the container should be deleted after spider
    running or timeout.

    crawl job sequence:
    1. Init virtualenv environment.
    2. Retrieve package requirements.
    3. Add configured extra requirements.
    4. Install requirements via venv pip.
    5. Generate running command according to spider settings.
    6. Run.
    7. Collect running result (Exit Code) and outputs, including item files, log files.
    8. Clear container.

    list job sequence:
    1. init
"""

import sys
import os
import shutil
import tempfile
from contextlib import contextmanager
from ..eggstorage import FilesystemEggStorage
import os
import pkg_resources
from six import next
import logging
from tornado.gen import coroutine, Return
from tornado.ioloop import IOLoop
import json
import docker
from scrapydd.workspace import SpiderSetting

logger = logging.getLogger(__name__)

class SpiderRunner(object):
    spider_settings = None
    venv_runner = None
    project_workspace = None
    def __init__(self, spider_settings):
        """
        Parameters:
            spider_settings (SpiderSetting): spider settings object.
        """
        self.spider_settings = spider_settings
        self._f_output = None
        self.output_file = None
        self.items_file = None
        self.ret_code = None
        self.workspace_dir = '.'
        self.output_file = os.path.join(self.workspace_dir, 'run.log')
        self._f_output = open(self.output_file, 'w')
        self.on_subprocess_start = None
        from ..workspace import ProjectWorkspace
        keep_files = True
        self.workspace = ProjectWorkspace(self.spider_settings.project_name, base_workdir=self.workspace_dir,
                                          keep_files=keep_files)
        self.keep_files = keep_files

    @coroutine
    def run(self):
        from ..exceptions import ProcessFailed
        try:
            yield self.workspace.init()
            yield self.workspace.install_requirements(self.spider_settings.extra_requirements)
            logger.info('start run spider.')
            run_spider_future = self.workspace.run_spider(self.spider_settings.spider_name,
                                                          self.spider_settings.spider_parameters,
                                                          f_output=self._f_output,
                                                          project=self.spider_settings.project_name)
            run_spider_pid = self.workspace.processes[0].pid
            self.items_file = yield run_spider_future
        except ProcessFailed as e:
            logger.warning('Process Failed when executing task %s: %s' % (self.task.id, e))
            error_log = e.message
            if e.std_output:
                logger.warning(e.std_output)
                error_log += e.std_output
            if e.err_output:
                logger.warning(e.err_output)
                error_log += e.err_output
        except Exception as e:
            logger.error('Error when executing task')
            logger.error(e)
            error_log = str(e)
        result = 0
        raise Return(result)

    def kill(self):
        self.workspace.kill_process()


class DockerRunner(object):
    container = None

    def run(self, spider_workingdir):
        client = docker.from_env()
        volumes = {
            spider_workingdir: {'bind': '/spider_run', 'mode': 'rw'}
        }
        container = client.containers.run("kevenli/scrapydd", ["python", "-m", "scrapydd.utils.runner2"], detach=True,
                                          volumes=volumes, working_dir='/spider_run')
        self.container = container

    def kill(self):
        self.container.stop()



def main():
    spider_setting = SpiderSetting.from_file('spider.json')
    runner = SpiderRunner(spider_setting)
    ioloop = IOLoop()
    ioloop.run_sync(runner.run)

def print_usage():
    print("usage:")
    print('runner2 <command> [options]')
    print('available commands:')
    print('    crawl')
    print('    list')
    print('')
    print('options:')
    print('-g, --egg egg_file             : specify spider egg file. Default is spider.egg in working folder.')
    print('-s, --settings settings_file   : specify the spider settings json file. Default is spider.json in ')
    print('                                 working folder.')


if __name__ == '__main__':
    main()

