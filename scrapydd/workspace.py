import sys
import os
import os.path
import logging
import tempfile
import shutil
import json
from io import BytesIO
from zipfile import ZipFile
from w3lib.url import path_to_file_uri
from shutil import copyfileobj
from six import ensure_str, ensure_binary
from os import path
from subprocess import Popen, PIPE
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado import gen
import docker
import requests
from scrapydd.exceptions import ProcessFailed, InvalidProjectEgg

logger = logging.getLogger(__name__)


class ProjectWorkspace(object):
    pip = None
    python = None
    processes = None
    project_workspace_dir = None
    project_check = None
    temp_dir = None

    def __init__(self, project_name, base_workdir=None, keep_files=False):
        if not base_workdir:
            base_workdir = tempfile.mkdtemp(prefix='scrapydd-tmp')
        project_workspace_dir = base_workdir
        self.venv_dir = tempfile.mkdtemp(prefix='scrapydd-tmp')
        self.project_workspace_dir = project_workspace_dir
        self.project_name = project_name
        self.keep_files = keep_files
        self.processes = []
        if sys.platform.startswith('linux'):
            self.pip = os.path.join(self.venv_dir, 'bin', 'pip')
            self.python = os.path.join(self.venv_dir, 'bin', 'python')
        elif sys.platform.startswith('win'):
            self.pip = os.path.join(self.venv_dir, 'Scripts', 'pip.exe')
            self.python = os.path.join(self.venv_dir, 'Scripts', 'python.exe')
        else:
            raise NotImplementedError('Unsupported system %s' % sys.platform)

    def init(self):
        """
        init project isolated workspace,
        :return: future
        """
        future = Future()
        if os.path.exists(self.pip) and os.path.exists(self.python):
            future.set_result(self)
            return future

        logger.debug('workspace dir : %s' % (self.project_workspace_dir,))
        logger.info('start creating virtualenv.')
        try:
            process = Popen([sys.executable, '-m', 'virtualenv', '--system-site-packages', self.venv_dir], stdout=PIPE,
                            stderr=PIPE)
            self.processes.append(process)
        except Exception as e:
            future.set_exception(e)
            return future

        def done(process):
            self.processes.remove(process)
            retcode = process.returncode
            if retcode is not None:
                if retcode == 0:
                    logger.info('Create virtualenv done.')
                    future.set_result(self)
                else:
                    std_output = process.stdout.read()
                    err_output = process.stderr.read()
                    future.set_exception(ProcessFailed('Error when init workspace virtualenv ', std_output=std_output,
                                                       err_output=err_output))

        wait_process(process, done)
        return future

    def find_project_requirements(self):
        try:
            eggf = open(os.path.join(self.project_workspace_dir, 'spider.egg'), 'rb')
        except IOError:
            raise PackageNotFoundException()
        with ZipFile(eggf) as egg_zip_file:
            try:
                requires_fileinfo = egg_zip_file.getinfo('EGG-INFO/requires.txt')
                return ensure_str(egg_zip_file.read(requires_fileinfo)).split()
            except KeyError:
                return []

    @gen.coroutine
    def install_requirements(self, extra_requirements=None):
        requirements = self.find_project_requirements()
        requirements += ['scrapydd']
        if extra_requirements:
            requirements += extra_requirements
        logger.info('start install requirements: %s.' % (requirements,))
        if requirements:
            yield self.pip_install(requirements)
        logger.info('install requirements done.')
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
            env['SCRAPY_EGG'] = 'spider.egg'
            process = Popen([self.python, '-m', 'scrapydd.utils.runner', 'list'], env=env, cwd=cwd, stdout=PIPE,
                            stderr=PIPE)
        except Exception as e:
            logger.error(e)
            future.set_exception(e)
            return future

        def check_process():
            logger.debug('poll')
            retcode = process.poll()
            if retcode is not None:
                if retcode == 0:
                    stdout = process.stdout.read()
                    stdout = ensure_str(stdout)
                    future.set_result(stdout.splitlines())
                else:
                    # future.set_exception(ProcessFailed(std_output=process.stdout.read(), err_output=process.stderr.read()))
                    future.set_exception(InvalidProjectEgg(detail=process.stderr.read()))
                return
            IOLoop.current().call_later(1, check_process)

        check_process()
        return future

    def settings_module(self):
        future = Future()
        cwd = self.project_workspace_dir
        try:
            env = os.environ.copy()
            env['SCRAPY_PROJECT'] = self.project_name
            env['SCRAPY_EGG'] = 'spider.egg'
            process = Popen([self.python, '-m', 'scrapydd.utils.extract_settings_module', 'spider.egg'],
                            env=env, cwd=cwd, stdout=PIPE,
                            stderr=PIPE)
        except Exception as e:
            logger.error(e)
            future.set_exception(e)
            return future

        def check_process():
            logger.debug('poll')
            retcode = process.poll()
            if retcode is not None:
                if retcode == 0:
                    stdout = process.stdout.read()
                    stdout = ensure_str(stdout).strip()
                    future.set_result(stdout)
                else:
                    # future.set_exception(ProcessFailed(std_output=process.stdout.read(), err_output=process.stderr.read()))
                    future.set_exception(InvalidProjectEgg(detail=process.stderr.read()))
                return
            IOLoop.current().call_later(1, check_process)

        check_process()
        return future

    def run_spider(self, spider, spider_parameters=None, f_output=None, project=None):
        ret_future = Future()
        items_file = os.path.join(self.project_workspace_dir, 'items.jl')
        runner = 'scrapydd.utils.runner'
        pargs = [self.python, '-m', runner, 'crawl', spider]
        if project:
            spider_parameters['BOT_NAME'] = project
        if spider_parameters:
            for spider_parameter_key, spider_parameter_value in spider_parameters.items():
                pargs += [
                    '-s',
                    '%s=%s' % (spider_parameter_key, spider_parameter_value)
                ]
        pargs += ['-o', str(path_to_file_uri(items_file))]

        env = os.environ.copy()
        env['SCRAPY_PROJECT'] = str(self.project_name)
        # env['SCRAPY_JOB'] = str(self.task.id)
        env['SCRAPY_FEED_URI'] = str(path_to_file_uri(items_file))
        env['SCRAPY_EGG'] = 'spider.egg'

        p = Popen(pargs, env=env, stdout=f_output, cwd=self.project_workspace_dir, stderr=f_output)
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
            process = Popen([self.python, '-m', 'scrapydd.utils.extract_settings', project], env=env, cwd=cwd,
                            stdout=PIPE, stderr=PIPE)
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

    def put_egg(self, eggfile, version=None):
        eggfile.seek(0)
        eggpath = os.path.join(self.project_workspace_dir, 'spider.egg')
        with open(eggpath, 'wb') as f:
            copyfileobj(eggfile, f)

    @gen.coroutine
    def kill_process(self, timeout=30):
        logger.info('killing process')
        for process in self.processes:
            if process.poll() is not None:
                continue
            try:
                process.terminate()
            except OSError as e:
                logger.error('Caught OSError when try to terminate subprocess: %s' % e)

            gen.sleep(timeout)

            if process.poll() is not None:
                continue
            try:
                process.kill()
            except OSError as e:
                logger.error('Caught OSError when try to kill subprocess: %s' % e)

    def __del__(self):
        if self.venv_dir and os.path.exists(self.venv_dir):
            shutil.rmtree(self.venv_dir)

        if self.keep_files:
            return

        if self.project_workspace_dir and os.path.exists(self.project_workspace_dir):
            shutil.rmtree(self.project_workspace_dir)


def wait_process(process, callback):
    retcode = process.poll()
    if retcode is not None:
        return callback(process)

    IOLoop.current().call_later(1, wait_process, process, callback)


class VenvRunner(object):
    _finished = False
    _work_dir = None
    _pip = None
    _python = None
    _project_workspace = None
    _spider_settings = None
    _prepare_finish = False
    debug = False

    def __init__(self, eggf):
        self._work_dir = tempfile.mkdtemp(prefix='scrapydd-tmp')
        self._project_workspace = ProjectWorkspace('spider')
        self._project_workspace.put_egg(eggf)

    @gen.coroutine
    def _prepare(self):
        if self._prepare_finish:
            raise gen.Return()
        yield self._project_workspace.init()
        requirements = self._project_workspace.find_project_requirements()
        if self._spider_settings:
            requirements += self._spider_settings.extra_requirements
        yield self._project_workspace.install_requirements(requirements)
        self._prepare_finish = True

    @gen.coroutine
    def list(self):
        yield self._prepare()
        ret = yield self._project_workspace.spider_list()
        raise gen.Return(ret)

    @gen.coroutine
    def crawl(self, spider_settings):
        """
        Parameters:
            spider_settings (SpiderSetting): spider settings object.
        """
        yield self._prepare()
        crawl_log_path = path.join(self._work_dir, 'crawl.log')
        f_crawl_log = open(crawl_log_path, 'w')
        try:
            ret = yield self._project_workspace.run_spider(spider_settings.spider_name,
                                                           spider_settings.spider_parameters,
                                                           f_output=f_crawl_log,
                                                           project=spider_settings.project_name)
            f_crawl_log.close()
            result = CrawlResult(0, items_file=ret, crawl_logfile=crawl_log_path)
            raise gen.Return(result)
        except ProcessFailed:
            f_crawl_log.close()
            with open(crawl_log_path, 'r') as f_log:
                error_log = f_log.read()
            raise ProcessFailed(err_output=error_log)


    @gen.coroutine
    def settings_module(self):
        yield self._prepare()
        ret = yield self._project_workspace.settings_module()
        raise gen.Return(ret)

    def kill(self):
        logger.info('killing process')
        self._project_workspace.kill_process()

    def clear(self):
        del self._project_workspace
        if os.path.exists(self._work_dir):
            shutil.rmtree(self._work_dir)


class DockerRunner(object):
    image = 'kevenli/scrapydd'
    _exit = False
    _container = None
    debug = False

    def __init__(self, eggf):
        self._client = docker.from_env()
        self._work_dir = tempfile.mkdtemp(prefix='scrapydd-tmp')
        eggf.seek(0)
        self._egg_file = os.path.join(self._work_dir, 'spider.egg')
        with open(self._egg_file, 'wb') as f:
            copyfileobj(eggf, f)
        self._ioloop = IOLoop.current()

    @property
    def in_container(self):
        return path.exists('/.dockerenv')

    # when controlling container in a container, cannot access
    # container's file directly, upload and download files
    # via api.
    def _put_egg(self, container):
        import tarfile
        stream = BytesIO()
        out = tarfile.open(fileobj=stream, mode='w')
        out.add(os.path.join(self._work_dir, 'spider.egg'), 'spider.egg')
        out.close()
        stream.seek(0)
        tar = stream.read()
        container.put_archive('/spider_run', tar)

    def _put_file(self, container, path, fileobj):
        import tarfile
        stream = BytesIO()
        out = tarfile.open(fileobj=stream, mode='w')
        tarinfo = tarfile.TarInfo(name=path)
        fileobj.seek(0, os.SEEK_END)
        tarinfo.size = fileobj.tell()
        fileobj.seek(0)
        out.addfile(tarinfo, fileobj)
        out.close()
        stream.seek(0)
        tar = stream.read()
        container.put_archive('/spider_run', tar)

    def _collect_files(self, container):
        import tarfile
        from io import BytesIO
        bits, stat = container.get_archive('/spider_run')
        stream = BytesIO()
        for chunk in bits:
            stream.write(chunk)
        stream.seek(0)
        out = tarfile.open(fileobj=stream, mode='r')
        for info in out.getmembers():
            logger.debug(info)
            if not info.isfile():
                continue
            extract_file = out.extractfile(info)
            with open(path.join(self._work_dir, path.basename(info.name)), 'wb') as f:
                f.write(extract_file.read())
        out.close()

    def _remove_container(self, container):
        if self._container == container:
            self._container = None

        if not self.debug:
            container.remove()

    def _start_container(self, container):
        if self._container is not None:
            raise Exception("Container is running.")
        self._container = container
        container.start()

    def _wait_container(self, container):
        try:
            ret_status = container.wait(timeout=0.1)
            ret_code = ret_status['StatusCode']
            return ret_code
        except requests.exceptions.ReadTimeout:
            return None
        # to hack the bug https://github.com/docker/docker-py/issues/1966
        # which raise an ConnectonError when read timeout
        except requests.exceptions.ConnectionError as e:
            return None

    @gen.coroutine
    def list(self):
        env = {'SCRAPY_EGG': 'spider.egg'}
        container = self._client.containers.create(self.image, ["python", "-m", "scrapydd.utils.runner", 'list'],
                                                   detach=True, working_dir='/spider_run',
                                                   environment=env)
        self._put_egg(container)
        self._start_container(container)
        ret_code = self._wait_container(container)
        while ret_code is None:
            yield gen.moment
            ret_code = self._wait_container(container)
        logs = container.logs()
        if ret_code == 0:
            self._collect_files(container)
            self._remove_container(container)
            raise gen.Return(ensure_str(logs).split())
        else:
            self._remove_container(container)
            raise ProcessFailed(err_output=logs)

    @gen.coroutine
    def crawl(self, spider_settings):
        """
        Parameters:
            spider_settings (SpiderSetting): spider settings object.

        Returns:
            Future: future

        As the settings values are forced writen with %(key)s = '%(value)s' format, the origin
        value type will lost, every settings values are strings. However, it would not be a
        problem since scrapy settings support a series of strong-typed settings value get interfaces.
        Always use strong-typed value retrieving method such as settings.getbool, getfloat, getint etc
        can prevent you from TypeError.

        see: https://docs.scrapy.org/en/latest/topics/api.html#scrapy.settings.BaseSettings.get

        """
        with open(path.join(self._work_dir, 'spider.json'), 'w') as f_settings:
            f_settings.write(spider_settings.to_json())
        items_file_path = path.join(self._work_dir, 'items.jl')
        log_file_path = path.join(self._work_dir, 'crawl.log')


        pargs = ["python", "-m", "scrapydd.utils.runner", 'crawl', spider_settings.spider_name,
                 '-o', 'items.jl']
        env = {}
        env['SCRAPY_FEED_URI'] = 'items.jl'
        env['SCRAPY_EGG'] = 'spider.egg'
        if spider_settings.base_settings_module:
            env['SCRAPY_SETTINGS_MODULE'] = 'settings'

        container = self._client.containers.create(self.image, pargs,
                                                   detach=True, working_dir='/spider_run',
                                                   environment=env)
        if spider_settings.base_settings_module:
            settings_module_file = BytesIO()
            settings_module_file.write(b'from %s import *\n' % ensure_binary(spider_settings.base_settings_module))
            for k, v in spider_settings.spider_parameters.items():
                settings_module_file.write(ensure_binary("%s='%s'\n" % (k, v)))
            settings_module_file.seek(0)
            self._put_file(container, 'settings.py', settings_module_file)

        self._put_egg(container)
        self._start_container(container)
        ret_code = self._wait_container(container)
        while ret_code is None:
            yield gen.moment
            ret_code = self._wait_container(container)

        process_output = ensure_str(container.logs())
        if ret_code == 0:
            with open(log_file_path, 'w') as f:
                f.write(process_output)
            self._collect_files(container)
            result = CrawlResult(0, items_file=items_file_path, crawl_logfile=log_file_path)
            self._remove_container(container)
            raise gen.Return(result)
        else:
            self._remove_container(container)
            raise ProcessFailed(err_output=process_output)

    @gen.coroutine
    def settings_module(self):
        pargs = ["python", "-m", "scrapydd.utils.extract_settings_module", 'spider.egg']
        container = self._client.containers.create(self.image, pargs,
                                                   detach=True, working_dir='/spider_run')
        self._put_egg(container)
        self._start_container(container)
        ret_code = self._wait_container(container)
        while ret_code is None:
            yield gen.moment
            ret_code = self._wait_container(container)

        output = ensure_str(container.logs()).strip()
        self._remove_container(container)
        if ret_code == 0:
            raise gen.Return(output)
        else:
            raise ProcessFailed(err_output=output)

    def kill(self):
        logger.info('killing process')
        if self._container:
            self._container.kill()

    def clear(self):
        if self.debug:
            return
        if self._container:
            self._container.remove()
        del self._container
        if os.path.exists(self._work_dir):
            shutil.rmtree(self._work_dir)


class RunnerFactory(object):
    _runner_type = 'venv'
    _docker_image = 'kevenli/scrapydd'
    _debug = False

    def __init__(self, config):
        self._runner_type = config.get('runner_type', 'venv')
        self._docker_image = config.get('runner_docker_image', 'kevenli/scrapydd')
        self._debug = config.getboolean('debug')

    def build(self, eggf):
        runner_type = self._runner_type
        if runner_type == 'venv':
            runner = VenvRunner(eggf)
        elif runner_type == 'docker':
            runner = DockerRunner(eggf)
            runner.image = self._docker_image
        else:
            raise Exception("Not supported runner_type: %s" % runner_type)
        runner.debug = self._debug
        return runner


class PackageNotFoundException(Exception):
    pass


class SpiderSetting(object):
    spider_name = None
    project_name = None
    extra_requirements = None
    spider_parameters = None
    base_settings_module = None

    def __init__(self, spider_name, extra_requirements=None, spider_parameters=None, project_name=None,
                 base_settings_module=None):
        self.spider_name = spider_name
        self.extra_requirements = extra_requirements or []
        self.spider_parameters = spider_parameters or {}
        self.project_name = project_name
        self.base_settings_module = base_settings_module

    def to_json(self):
        d = {
            'spider_name': self.spider_name,
            'project_name': self.project_name,
            'extra_requirements': self.extra_requirements,
            'spider_parameters': self.spider_parameters,
            'base_settings_module': self.base_settings_module
        }
        return json.dumps(d)

    @classmethod
    def from_json(cls, json_str):
        parsed = json.loads(json_str)
        return SpiderSetting.from_dict(parsed)

    @classmethod
    def from_dict(cls, dic):
        """
        type: (cls, dict) -> SpiderSetting
        """
        spider_name = dic['spider_name']
        project_name = dic.get('project_name')
        extra_requirements = dic.get('extra_requirements')
        spider_parameters = dic.get('spider_parameters')
        base_settings_module = dic.get('base_settings_module')

        return cls(spider_name, extra_requirements, spider_parameters, project_name,
                   base_settings_module=base_settings_module)

    @classmethod
    def from_file(cls, file_path):
        with open(file_path, 'r') as f:
            json_content = f.read()
            return SpiderSetting.from_json(json_content)


class CrawlResult(object):
    _items_file = None
    _crawl_logfile = None
    _ret_code = 0
    _console_output = None

    def __init__(self, ret_code, error_message=None, items_file=None, crawl_logfile=None):
        self._ret_code = ret_code
        self._error_message = error_message
        self._items_file = items_file
        self._crawl_logfile = crawl_logfile

    @property
    def ret_code(self):
        return self._ret_code

    @property
    def error_message(self):
        return self._error_message

    @property
    def items_file(self):
        return self._items_file

    @property
    def crawl_logfile(self):
        return self._crawl_logfile
