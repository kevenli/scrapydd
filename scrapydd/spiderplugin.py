import subprocess
import sys
import json
import tempfile
import shutil
import os
import venv
from tornado import gen
from .models import SysSpiderPlugin, SysSpiderPluginParameter, session_scope
from .exceptions import ProcessFailed


class SpiderPluginManager:
    def add_sys_plugin(self, egg_f):
        pass

    def _init_venv(self, work_dir):
        builder = venv.EnvBuilder(with_pip=True)
        builder.create(work_dir)
        if sys.platform == 'win32':
            bin_dir = 'Scripts'
        else:
            bin_dir = 'bin'
        return os.path.join(work_dir, bin_dir)


    @gen.coroutine
    def get_plugin_info(self, egg_f, plugin_name):
        user_home = os.environ.get('HOME')
        plugin_cache_dir = os.path.join(user_home, '.cache', 'scrapydd',
                                        'spiderplugins',
                                        plugin_name)
        if not os.path.exists(plugin_cache_dir):
            os.makedirs(plugin_cache_dir)
        work_dir = os.path.join(plugin_cache_dir, 'venv')
        tmp_egg_path = os.path.join(plugin_cache_dir, 'spiderplugin.egg')
        with open(tmp_egg_path, 'wb') as f_temp_egg:
            shutil.copyfileobj(egg_f, f_temp_egg)

        py_file = os.path.join(os.path.dirname(__file__), 'utils', 'plugin.py')
        dest_file = 'plugin.py'
        shutil.copyfile(py_file, os.path.join(plugin_cache_dir, dest_file))

        venv_dir = self._init_venv(work_dir)
        executable = os.path.join(venv_dir, 'python')

        p_args = [executable,
                  dest_file,
                  'desc',
                  tmp_egg_path
                  ]
        p = subprocess.Popen(p_args,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             cwd=plugin_cache_dir)
        while True:
            ret = p.poll()
            if ret is None:
                yield gen.sleep(1)
                continue
            if ret == 0:
                raise gen.Return(json.loads(p.stdout.read()))
            else:
                raise ProcessFailed(message=p.stderr.read())
