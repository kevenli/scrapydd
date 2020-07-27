import sys
import os
import os.path
import asyncio
import logging
import tempfile
import shutil
import json
from io import BytesIO
from zipfile import ZipFile
from typing import Union
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

PROCESS_ENCODING = 'utf8'


class SpiderSetting(object):
    spider_name = None
    project_name = None
    extra_requirements = None
    spider_parameters = None
    base_settings_module = None
    output = None
    egg_path = None

    def __init__(self, spider_name, extra_requirements=None, spider_parameters=None, project_name=None,
                 base_settings_module=None,
                 output=None,
                 plugin_settings=None, **kwargs):
        self.spider_name = spider_name
        if extra_requirements and isinstance(extra_requirements, str):
            extra_requirements = [x for x in
                                  extra_requirements.split(';') if x]
        self.extra_requirements = extra_requirements or []
        self.spider_parameters = spider_parameters or {}
        self.project_name = project_name
        self.base_settings_module = base_settings_module
        self.output = output
        self.plugin_settings = plugin_settings or {}
        self.plugins = kwargs.get('plugins') or []
        self.package = kwargs.get('package')

    def to_json(self):
        d = {
            'spider': self.spider_name,
            'project_name': self.project_name,
            'extra_requirements': self.extra_requirements,
            'spider_parameters': self.spider_parameters,
            'base_settings_module': self.base_settings_module,
            'plugin_settings': self.plugin_settings,
            'package': self.package,
        }
        if self.output:
            d['output'] = self.output
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
        spider_name = dic.get('spider_name') or dic.get('spider')
        project_name = dic.get('project_name')
        extra_requirements = dic.get('extra_requirements')
        spider_parameters = dic.get('spider_parameters')
        base_settings_module = dic.get('base_settings_module')
        output = dic.get('output')
        plugin_settings = dic.get('plugin_settings')


        return cls(spider_name, extra_requirements, spider_parameters,
                   project_name,
                   base_settings_module=base_settings_module,
                   output=output,
                   plugin_settings=plugin_settings,
                   plugins=dic.get('plugins'),
                   package=dic.get('package'))

    @property
    def spider(self):
        return self.spider_name


class DictSpiderSettings(dict):
    def to_json(self):
        return json.dumps(self)

    @property
    def spider(self):
        return self.get('spider')


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
            logger.debug('creating workspace dir in %s', base_workdir)
        project_workspace_dir = base_workdir
        self.venv_dir = tempfile.mkdtemp(prefix='scrapydd-tmp')
        logger.debug('venv_dir: %s', self.venv_dir)
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

    async def init(self):
        """
        init project isolated workspace,
        :return: future
        """
        if os.path.exists(self.pip) and os.path.exists(self.python):
            return

        logger.debug('workspace dir : %s' % (self.project_workspace_dir,))
        logger.info('start creating virtualenv.')
        try:
            stdout_stream = tempfile.NamedTemporaryFile(dir=self.project_workspace_dir)
            stderr_stream = tempfile.NamedTemporaryFile(dir=self.project_workspace_dir)
            env = os.environ
            process = Popen([sys.executable, '-m', 'virtualenv',
                             '--system-site-packages', self.venv_dir],
                            env=env,
                            stdout=stdout_stream, stderr=stderr_stream)
            logger.debug('pid: %s', process.pid)

            ret_code = await self._wait_process(process)
        finally:
            if stdout_stream:
                stdout_stream.close()
            if stderr_stream:
                stderr_stream.close()

        if ret_code != 0:
            raise ProcessFailed(ret_code=ret_code)
        return

    def find_project_requirements(self):
        eggf = None
        try:
            file_path = os.path.join(self.project_workspace_dir, 'spider.egg')
            eggf = open(file_path, 'rb')
            with ZipFile(eggf) as egg_zip_file:
                try:
                    requires_fileinfo = egg_zip_file.getinfo(
                        'EGG-INFO/requires.txt')
                    return ensure_str(
                        egg_zip_file.read(requires_fileinfo)).split()
                except KeyError:
                    return []
        except IOError:
            raise PackageNotFoundException()
        finally:
            if eggf:
                eggf.close()

    @gen.coroutine
    def install_requirements(self, extra_requirements=None):
        requirements = self.find_project_requirements()
        requirements += ['pancli', 'scrapy']
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
            env = os.environ.copy()
            stdout_stream = tempfile.NamedTemporaryFile()
            stderr_stream = tempfile.NamedTemporaryFile()
            process = Popen([self.pip, 'install'] + requirements,
                            stdout=stdout_stream, stderr=stderr_stream,
                            env=env,
                            encoding=PROCESS_ENCODING)
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
                    stdout_stream.seek(0)
                    stderr_stream.seek(0)
                    std_out = stdout_stream.read().decode(PROCESS_ENCODING)
                    err_out = stderr_stream.read().decode(PROCESS_ENCODING)
                    future.set_exception(ProcessFailed(ret_code=retcode,
                                                       std_output=std_out,
                                                       err_output=err_out))

        wait_process(process, done)
        return future

    def spider_list(self):
        future = Future()
        cwd = self.project_workspace_dir
        try:
            env = os.environ.copy()
            args = [self.python, '-m', 'pancli.cli', 'list',
                    '--package', 'spider.egg']
            process = Popen(args,
                            cwd=cwd, stdout=PIPE,
                            stderr=PIPE, encoding=PROCESS_ENCODING)
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
                    stdout = process.stdout.read()
                    logger.info(stdout)
                    stderr = process.stderr.read()
                    logger.warning(stderr)
                    # future.set_exception(ProcessFailed(std_output=process.stdout.read(), err_output=process.stderr.read()))
                    future.set_exception(InvalidProjectEgg(detail=process.stderr.read()))
                return
            IOLoop.current().call_later(1, check_process)

        check_process()
        return future

    async def run_spider(self, spider, f_output=None,
                         spider_settings=None):
        items_file = 'items.jl'
        items_file_path = os.path.join(self.project_workspace_dir, items_file)

        spider_json = 'spider.json'
        spider_json_path = os.path.join(self.project_workspace_dir, spider_json)

        with open(spider_json_path, 'w') as f:
            f.write(spider_settings.to_json())

        pargs = [self.python, '-m', 'pancli.cli', 'crawl', spider,
                 '-f', 'spider.json',
                 '--package', 'spider.egg']

        pargs += ['-o', items_file]
        logger.debug(pargs)

        p = Popen(pargs, stdout=f_output,
                  cwd=self.project_workspace_dir,
                  stderr=f_output, encoding='utf8')
        retcode = await self._wait_process(p)
        if retcode != 0:
            logger.debug('ret_code %s', retcode)
            raise ProcessFailed(ret_code=retcode)
        ret = CrawlResult(ret_code=retcode, items_file=items_file_path,
                          runner=self)
        return ret

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

    async def kill_process(self, timeout=30):
        logger.info('killing process')
        for process in self.processes:
            if process.poll() is not None:
                continue
            try:
                process.terminate()
            except OSError as e:
                logger.error('Caught OSError when try to terminate subprocess: %s' % e)

            await asyncio.sleep(timeout)

            if process.poll() is not None:
                continue
            try:
                process.kill()
            except OSError as e:
                logger.error('Caught OSError when try to kill subprocess: %s' % e)

    async def _wait_process(self, process: Popen):
        self.processes.append(process)
        ret_code = process.poll()
        while ret_code is None:
            await asyncio.sleep(1)
            logger.debug('polling process %d', process.pid)
            ret_code = process.poll()
        try:
            self.processes.remove(process)
        except ValueError:
            logger.debug('process not found.')
            pass
        return ret_code


    def __del__(self):
        logger.debug('Cleaning project workspace.')
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
    def crawl(self, spider_settings: SpiderSetting):
        """
        Parameters:
            spider_settings (SpiderSetting): spider settings object.
        """
        self._spider_settings = spider_settings
        yield self._prepare()
        crawl_log_path = path.join(self._work_dir, 'crawl.log')
        f_crawl_log = open(crawl_log_path, 'w')
        try:
            ret = yield self._project_workspace.run_spider(spider_settings.spider,
                                                           f_output=f_crawl_log,
                                                           spider_settings=spider_settings)
            f_crawl_log.close()
            ret.crawl_logfile = crawl_log_path
            raise gen.Return(ret)
        except ProcessFailed as e:
            f_crawl_log.close()
            with open(crawl_log_path, 'r') as f_log:
                error_log = f_log.read()
            ret = CrawlResult(ret_code=e.ret_code,
                              crawl_logfile=f_crawl_log)
            raise gen.Return(ret)

    async def kill(self):
        logger.info('killing process')
        await self._project_workspace.kill_process()

    def clear(self):
        self._project_workspace = None
        if os.path.exists(self._work_dir):
            shutil.rmtree(self._work_dir)


class DockerRunner(object):
    image = 'pansihub/pancli'
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
        logger.debug('DockerRunner._remove_container')
        if self._container == container:
            self._container = None

        if not self.debug:
            container.remove()
        else:
            logger.debug('keep container in debug mode.')

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
        pargs = ["python", "-m", "pancli.cli", 'list', '--package', 'spider.egg']
        container = self._client.containers.create(self.image, pargs,
                                                   detach=True,
                                                   working_dir='/spider_run')
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
            raise ProcessFailed(ret_code=ret_code, err_output=logs)

    @gen.coroutine
    def crawl(self, spider_settings: Union[SpiderSetting]):
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
        settings_content = spider_settings.to_json()
        spider_json_buffer = BytesIO()
        spider_json_buffer.write(settings_content.encode('utf8'))
        spider_json_buffer.seek(0)
        items_file_path = path.join(self._work_dir, 'items.jl')
        log_file_path = path.join(self._work_dir, 'crawl.log')

        pargs = ["python",
                 "-m",
                 "pancli.cli",
                 "crawl",
                 spider_settings.spider,
                 "-f",
                 "spider.json",
                 '--output',
                 'items.jl',
                 '--package',
                 'spider.egg'
                 ]

        container = self._client.containers.create(self.image, pargs,
                                                   detach=True,
                                                   working_dir='/spider_run')
        self._put_file(container, 'spider.json', spider_json_buffer)
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
            raise ProcessFailed(ret_code=ret_code, err_output=process_output)

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
            raise ProcessFailed(ret_code=ret_code, err_output=output)

    def kill(self):
        logger.info('killing process')
        if self._container:
            self._container.kill()

    def clear(self):
        if self.debug:
            return
        if self._container:
            self._container.remove()
            self._container = None
        if os.path.exists(self._work_dir):
            shutil.rmtree(self._work_dir)


class RunnerFactory(object):
    def __init__(self, config):
        self._runner_type = config.get('runner_type', 'venv')
        self._docker_image = config.get('runner_docker_image', 'pansihub/pancli')
        self._debug = config.getboolean('debug', 'false')

        # try check docker image exists.
        # It will not retrieve image at runtime.
        if self._runner_type == 'docker':
            client = docker.from_env()
            image = client.images.get(self._docker_image)
            client = None

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





class CrawlResult(object):
    _items_file = None
    _crawl_logfile = None
    _ret_code = 0
    _console_output = None
    _runner = None

    def __init__(self, ret_code, error_message=None, items_file=None,
                 crawl_logfile=None, runner=None):
        """

        :param ret_code:
        :param error_message:
        :param items_file:
        :param crawl_logfile:
        :param runner: can be anything which holds the resources before
                    result has been processed. For example an object
                    where holds a temp folder containing item and log
                    files, can delete the entire folder when __del__
                    is called.
        """
        self._ret_code = ret_code
        self._error_message = error_message
        self._items_file = items_file
        self.crawl_logfile = crawl_logfile

    @property
    def ret_code(self):
        return self._ret_code

    @property
    def error_message(self):
        return self._error_message

    @property
    def items_file(self):
        return self._items_file
