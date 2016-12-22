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
from scrapydd.exceptions import ProcessFailed, InvalidProjectEgg


logger = logging.getLogger(__name__)


class ProjectWorkspace(object):
    pip = None
    python = None
    process = None
    project_workspace_dir = None
    project_check = None
    temp_dir = None

    def __init__(self, project_name):
        project_workspace_dir = os.path.abspath(os.path.join('workspace', project_name))
        self.project_workspace_dir = project_workspace_dir
        self.project_name = project_name
        self.egg_storage = FilesystemEggStorage(scrapyd.config.Config())
        if sys.platform.startswith('linux'):
            self.pip = os.path.join(project_workspace_dir, 'bin', 'pip')
            self.python = os.path.join(project_workspace_dir, 'bin', 'python')
        elif sys.platform.startswith('win'):
            self.pip = os.path.join(project_workspace_dir, 'Scripts', 'pip.exe')
            self.python = os.path.join(project_workspace_dir, 'Scripts', 'python.exe')
        else:
            raise NotImplementedError('Unsupported system %s' % sys.platform)

    def init(self):
        '''
        init project isolated workspace,
        :return: future
        '''
        future = Future()
        if os.path.exists(self.pip) and os.path.exists(self.python):
            future.set_result(self)
            return future

        try:
            process = Popen(['virtualenv', '--system-site-packages', self.project_workspace_dir], stdout=PIPE, stderr=PIPE)
        except Exception as e:
            future.set_exception(e)
            return future
        def check_process():
            logger.debug('poll')
            retcode = process.poll()
            if retcode is not None:
                if retcode == 0:
                    future.set_result(self)
                else:
                    std_output = process.stdout.read()
                    err_output = process.stderr.read()
                    future.set_exception(ProcessFailed('Error when init workspace virtualenv ', std_output=std_output, err_output=err_output))
                return
            IOLoop.current().call_later(1, check_process)

        check_process()
        return future

    def find_project_requirements(self, project, egg_storage=None, eggf=None):
        if eggf is None:
            if egg_storage is None:
                egg_storage = FilesystemEggStorage(scrapyd.config.Config())
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

    def test_egg(self, eggf):
        future = Future()
        temp_dir = tempfile.mkdtemp('scrapydd-egg-%s' % self.project_name)
        self.temp_dir = temp_dir
        eggf.seek(0)
        egg_storage = FilesystemEggStorage(scrapyd.config.Config({'eggs_dir': os.path.join(temp_dir, 'eggs')}))
        egg_storage.put(eggf, project=self.project_name, version='1')
        eggf.seek(0)

        requirements = self._read_egg_requirements(eggf) + ['scrapyd']

        def after_spider_list(callback_future):
            logger.debug('after_spider_list')
            exc = callback_future.exception()
            if exc is not None:
                future.set_exception(exc)
                return
            spider_list = callback_future.result()
            #os.removedirs(temp_dir)
            future.set_result(spider_list)

        def after_pip_install(callback_future):
            logger.debug('after_pip_install')
            exc = callback_future.exception()
            if exc is not None:
                future.set_exception(exc)
                return

            self.spider_list(self.project_name, cwd=temp_dir).add_done_callback(after_spider_list)

        self.pip_install(requirements).add_done_callback(after_pip_install)

        return future

    def _read_egg_requirements(self, eggf):
        try:
            prefix = '%s-%s-' % (self.project_name, 0)
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
        logger.debug('installing requirements: %s' % requirements)
        future = Future()
        try:
            process = Popen([self.pip, 'install'] + requirements, stdout=PIPE, stderr=PIPE)
        except Exception as e:
            future.set_exception(e)
            return future
        def check_process():
            logger.debug('poll')
            retcode = process.poll()
            if retcode is not None:
                if retcode == 0:
                    future.set_result(self)
                else:
                    std_out = process.stdout.read()
                    err_out = process.stderr.read()
                    future.set_exception(ProcessFailed(std_output=std_out, err_output=err_out))
                return
            IOLoop.current().call_later(1, check_process)

        check_process()
        return future

    def spider_list(self, project, cwd=None):
        future = Future()
        try:
            env = os.environ.copy()
            env['SCRAPY_PROJECT'] = project
            process = Popen([self.python, '-m', 'scrapyd.runner', 'list'], env = env, cwd=cwd, stdout = PIPE, stderr= PIPE)
        except Exception as e:
            logger.error(e)
            future.set_exception(e)
            return future

        def check_process():
            logger.debug('poll')
            retcode = process.poll()
            if retcode is not None:
                if retcode == 0:
                    future.set_result(process.stdout.read().splitlines())
                else:
                    #future.set_exception(ProcessFailed(std_output=process.stdout.read(), err_output=process.stderr.read()))
                    future.set_exception(InvalidProjectEgg(detail=process.stderr.read()))
                return
            IOLoop.current().call_later(1, check_process)

        check_process()
        return future

    def clearup(self):
        '''
        clean up temp files.
        :return:
        '''
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def put_egg(self, eggfile, version):
        eggfile.seek(0)
        self.egg_storage.put(eggfile=eggfile, project=self.project_name, version=version)

    def get_egg(self, version=None):
        return self.egg_storage.get(self.project_name, version=version)

    def delete_egg(self, project, version=None):
        logger.info('deleting project eggs')
        return self.egg_storage.delete(project, version)

    def list_versions(self, project):
        return self.egg_storage.list(project)
