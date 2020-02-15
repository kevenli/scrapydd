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
    def __init__(self, data_dir='data'):
        self._data_dir = data_dir

    @gen.coroutine
    def add_sys_plugin(self, egg_f, plugin_name):
        with session_scope() as session:
            plugin_info = yield self.get_plugin_info(egg_f, plugin_name)
            egg_f.seek(0)
            plugin = session.query(SysSpiderPlugin)\
                .filter_by(name=plugin_name)\
                .first()
            plugin_dir = os.path.join(self._data_dir, 'sys_spider_plugins')
            if not os.path.exists(plugin_dir):
                os.makedirs(plugin_dir)
            location = os.path.join(plugin_dir, '%s.egg' % plugin_name)

            with open(location, 'wb') as f:
                shutil.copyfileobj(egg_f, f)
            if plugin is None:
                plugin = SysSpiderPlugin(name=plugin_name, location=location)

            session.add(plugin)
            session.flush()
            session.refresh(plugin)
            session.query(SysSpiderPluginParameter)\
                .filter_by(plugin_id=plugin.id)\
                .delete()
            for parameter_key, parameter_settings in plugin_info['parameters'].items():
                parameter = SysSpiderPluginParameter()
                parameter.plugin_id = plugin.id
                parameter.key = parameter_key
                parameter.datatype = parameter_settings['type']
                parameter.required = parameter_settings.get('required', False)
                parameter_default_value = parameter_settings.get('default_value')
                if parameter_default_value:
                    parameter_default_value = str(parameter_default_value)
                parameter.default_value = parameter_default_value
                session.add(parameter)
            session.commit()

    def get_plugin(self, plugin_name):
        with session_scope() as session:
            plugin = session.query(SysSpiderPlugin) \
                .filter_by(name=plugin_name) \
                .first()
            plugin.parameters
            return plugin

    def get_all_plugins(self):
        with session_scope() as session:
            plugins = list(session.query(SysSpiderPlugin).all())
            for plugin in plugins:
                plugin.parameters
            return plugins

    def _init_venv(self, work_dir):
        if not os.path.exists(work_dir):
            builder = venv.EnvBuilder(system_site_packages=True, with_pip=True)
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
        venv_dir = self._init_venv(work_dir)
        executable = os.path.join(venv_dir, 'python')

        p_args = [executable,
                  py_file,
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
