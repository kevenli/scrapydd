import subprocess
import sys
import json
import tempfile
import shutil
import os

from tornado import gen
from .models import SysSpiderPlugin, SysSpiderPluginParameter, session_scope
from .exceptions import ProcessFailed


class SpiderPluginManager:
    def add_sys_plugin(self, egg_f):
        pass

    @gen.coroutine
    def _init_venv(self, work_dir):
        process = subprocess.Popen(
            [sys.executable, '-m', 'virtualenv', '--system-site-packages',
             work_dir], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        while True:
            ret = process.poll()
            if ret is None:
                yield gen.sleep(1)
                continue
            if ret == 0:
                if sys.platform == 'win32':
                    bin = 'Scripts'
                else:
                    bin = 'bin'
                raise gen.Return(os.path.join(work_dir, bin))
            else:
                raise ProcessFailed(message=process.stderr.read())


    @gen.coroutine
    def get_plugin_info(self, egg_f):
        work_dir = tempfile.mkdtemp()
        tmp_egg_path = tempfile.mktemp(dir=work_dir, suffix='.egg')
        with open(tmp_egg_path, 'wb') as f_temp_egg:
            shutil.copyfileobj(egg_f, f_temp_egg)

        venv_dir = yield self._init_venv(work_dir)
        executable = os.path.join(venv_dir, 'python')

        p_args = [executable,
                  '-m',
                  'scrapydd.utils.plugin',
                  'desc',
                  tmp_egg_path
                  ]
        p = subprocess.Popen(p_args,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        while True:
            ret = p.poll()
            if ret is None:
                yield gen.sleep(1)
                continue
            if ret == 0:
                shutil.rmtree(work_dir)
                raise gen.Return(json.loads(p.stdout.read()))
            else:
                shutil.rmtree(work_dir)
                raise ProcessFailed(message=p.stderr.read())
