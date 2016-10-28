import sys
import os, os.path
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from subprocess import Popen, PIPE
import logging
import tempfile
import scrapyd.config
from scrapyd.eggstorage import FilesystemEggStorage
import shutil
import pkg_resources


logger = logging.getLogger(__name__)

class ProjectWorkspace():
    pip = None
    python = None
    process = None
    project_workspace_dir = None
    project_check = None

    def __init__(self, project_name):
        project_workspace_dir = os.path.abspath(os.path.join('workspace', project_name))
        self.project_workspace_dir = project_workspace_dir
        if sys.platform.startswith('linux'):
            self.pip = os.path.join(project_workspace_dir, 'bin', 'pip')
            self.python = os.path.join(project_workspace_dir, 'bin', 'python')
        elif sys.platform.startswith('win'):
            self.pip = os.path.join(project_workspace_dir, 'Scripts', 'pip.exe')
            self.python = os.path.join(project_workspace_dir, 'Scripts', 'python.exe')
        else:
            raise NotImplementedError('Unsupported system %s' % sys.platform)

    def init(self):
        future = Future()
        if os.path.exists(self.pip) and os.path.exists(self.python):
            future.set_result(self)
            return future

        try:
            process = Popen(['virtualenv', '--system-site-packages', self.project_workspace_dir])
        except Exception as e:
            future.set_exception(e)
        def check_process():
            logger.debug('poll')
            retcode = process.poll()
            if retcode is not None:
                if retcode == 0:
                    future.set_result(self)
                else:
                    future.set_exception(Exception('Error when init workspace virtualenv '))
                return
            IOLoop.current().call_later(1, check_process)

        check_process()
        return future

    def find_project_requirements(self, project, egg_storage=None, eggf=None):
        if eggf is None:

            version, eggf = egg_storage.get(project)
        try:
            prefix = '%s-nover-' % (project)
            fd, eggpath = tempfile.mkstemp(prefix=prefix, suffix='.egg')
            logger.debug('tmp egg file saved to %s' % eggpath)
            lf = os.fdopen(fd, 'wb')
            eggf.seek(0)
            shutil.copyfileobj(eggf, lf)
            lf.close()
            try:
                d = pkg_resources.find_distributions(eggpath).next()
            except StopIteration:
                raise ValueError("Unknown or corrupt egg")
            requirements = [str(x) for x in d.requires()]
            return requirements
        finally:
            if eggpath:
                os.remove(eggpath)

    def pip_install(self, requirements):
        future = Future()
        try:
            process = Popen([self.pip, 'install'] + requirements)
        except Exception as e:
            future.set_exception(e)
        def check_process():
            logger.debug('poll')
            retcode = process.poll()
            if retcode is not None:
                if retcode == 0:
                    future.set_result(self)
                else:
                    future.set_exception(Exception('Error when init workspace virtualenv '))
                return
            IOLoop.current().call_later(1, check_process)

        check_process()
        return future

    def spider_list(self, project):
        future = Future()
        try:
            env = os.environ.copy()
            env['SCRAPY_PROJECT'] = project
            process = Popen([self.python, '-m', 'scrapyd.runner', 'list'], env = env, stdout = PIPE)
        except Exception as e:
            future.set_exception(e)

        def check_process():
            logger.debug('poll')
            retcode = process.poll()
            if retcode is not None:
                if retcode == 0:
                    future.set_result(process.stdout.read().splitlines())
                else:
                    future.set_exception(Exception('Error when run spider_list.'))
                return
            IOLoop.current().call_later(1, check_process)

        check_process()
        return future