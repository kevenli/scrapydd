import sys
import os, os.path
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado import gen
from subprocess import Popen, PIPE
import logging
import tempfile
from .eggstorage import FilesystemEggStorage
import shutil
from scrapydd.exceptions import ProcessFailed, InvalidProjectEgg
import json
from zipfile import ZipFile
from w3lib.url import path_to_file_uri


logger = logging.getLogger(__name__)

class ProjectWorkspace(object):
    pip = None
    python = None
    processes = None
    project_workspace_dir = None
    project_check = None
    temp_dir = None

    def __init__(self, project_name, base_workdir=None):
        if not base_workdir:
            base_workdir = tempfile.mkdtemp(prefix='scrapydd-tmp')
        project_workspace_dir = os.path.abspath(os.path.join(base_workdir, 'workspace', project_name))
        self.project_workspace_dir = project_workspace_dir
        self.project_name = project_name
        self.egg_storage = FilesystemEggStorage({'eggs_dir':os.path.join(project_workspace_dir, 'eggs')})
        self.processes = []
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

        logger.info('start creating virtualenv.')
        try:
            process = Popen(['virtualenv', '--system-site-packages', self.project_workspace_dir], stdout=PIPE, stderr=PIPE)
            self.processes.append(process)
        except Exception as e:
            future.set_exception(e)
            return future

        def done(process):
            self.processes.remove(process)
            retcode = process.returncode
            if retcode is not None:
                if retcode == 0:
                    future.set_result(self)
                else:
                    std_output = process.stdout.read()
                    err_output = process.stderr.read()
                    future.set_exception(ProcessFailed('Error when init workspace virtualenv ', std_output=std_output, err_output=err_output))

        wait_process(process, done)
        return future

    def find_project_requirements(self, project=None, egg_storage=None, eggf=None):
        project = self.project_name
        if eggf is None:
            if egg_storage is None:
                egg_storage = self.egg_storage
            version, eggf = egg_storage.get(project)
        with ZipFile(eggf) as egg_zip_file:
            try:
                requires_fileinfo = egg_zip_file.getinfo('EGG-INFO/requires.txt')
                return egg_zip_file.read(requires_fileinfo).split()
            except KeyError:
                return []

    @gen.coroutine
    def install_requirements(self, extra_requirements=None):
        requirements = self.find_project_requirements(self.project_name)
        requirements += ['scrapydd', 'scrapyd']
        if extra_requirements:
            requirements += extra_requirements
        if requirements:
            yield self.pip_install(requirements)

        raise gen.Return()

    def pip_install(self, requirements):
        logger.debug('installing requirements: %s' % requirements)
        future = Future()
        try:
            process = Popen([self.pip, 'install'] + requirements, stdout=PIPE, stderr=PIPE)
            self.processes.append(process)
        except Exception as e:
            future.set_exception(e)
            return future

        def done(process):
            self.processes.remove(process)
            retcode = process.returncode
            if retcode is not None:
                if retcode == 0:
                    future.set_result(self)
                else:
                    std_out = process.stdout.read()
                    err_out = process.stderr.read()
                    future.set_exception(ProcessFailed(std_output=std_out, err_output=err_out))

        wait_process(process, done)
        return future

    def spider_list(self):
        future = Future()
        cwd = self.project_workspace_dir
        try:
            env = os.environ.copy()
            env['SCRAPY_PROJECT'] = self.project_name
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

    def run_spider(self, spider, spider_parameters=None, f_output=None, project=None):
        ret_future = Future()
        items_file = os.path.join(self.project_workspace_dir, 'items.jl')
        runner = 'scrapyd.runner'
        pargs = [self.python, '-m', runner, 'crawl', spider]
        if project:
            spider_parameters['BOT_NAME'] = project
        if spider_parameters:
            for spider_parameter_key, spider_parameter_value in spider_parameters.items():
                pargs += [
                            '-s',
                            '%s=%s' % (spider_parameter_key, spider_parameter_value)
                          ]

        env = os.environ.copy()
        env['SCRAPY_PROJECT'] = str(self.project_name)
        #env['SCRAPY_JOB'] = str(self.task.id)
        env['SCRAPY_FEED_URI'] = str(path_to_file_uri(items_file))

        p = Popen(pargs, env = env, stdout = f_output, cwd = self.project_workspace_dir, stderr = f_output)
        self.processes.append(p)

        def done(process):
            self.processes.remove(process)
            if process.returncode:
                return ret_future.set_exception(ProcessFailed())

            return ret_future.set_result(items_file)

        wait_process(p, done)
        return ret_future

    def find_project_settings(self, project):
        cwd = self.project_workspace_dir
        future = Future()
        try:
            env = os.environ.copy()
            env['SCRAPY_PROJECT'] = project
            process = Popen([self.python, '-m', 'scrapydd.utils.extract_settings', project], env = env, cwd=cwd, stdout = PIPE, stderr= PIPE)
            self.processes.append(process)
        except Exception as e:
            logger.error(e)
            future.set_exception(e)
            return future

        def done(process):
            retcode = process.poll()
            self.processes.remove(process)
            if retcode == 0:
                proc_output = process.stdout.read()
                logger.debug('find_project_settings output:')
                logger.debug(proc_output)
                return future.set_result(json.loads(proc_output))
            else:
                # future.set_exception(ProcessFailed(std_output=process.stdout.read(), err_output=process.stderr.read()))
                return future.set_exception(InvalidProjectEgg(detail=process.stderr.read()))

        wait_process(process, done)
        return future

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

    @gen.coroutine
    def kill_process(self):
        logger.info('killing process')
        for process in self.processes:
            if process.poll() is not None:
                continue
            try:
                process.terminate()
            except OSError as e:
                logger.error('Caught OSError when try to terminate subprocess: %s' % e)

            gen.sleep(10)

            if process.poll() is not None:
                continue
            try:
                process.kill()
            except OSError as e:
                logger.error('Caught OSError when try to kill subprocess: %s' % e)

    def __del__(self):
        if self.project_workspace_dir and os.path.exists(self.project_workspace_dir):
            shutil.rmtree(self.project_workspace_dir)

def wait_process(process, callback):
    retcode = process.poll()
    if retcode is not None:
        return callback(process)

    IOLoop.current().call_later(1, wait_process, process, callback)