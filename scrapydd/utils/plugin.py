"""
This module is design to be called by command-line or subprocesses
to interact with SpiderPlugins, it is self-contained, so that
it can be used alone to test plugin or play with scrapy spiders
without scrapydd environment.

Scrapydd call this module in virtualenv or docker environment to isolate
plugins from main process environment.

To test a plugin:
1. Create a plugin settings json file (settings.json) manually or by
  `python -m scrpaydd.utils.plugin generate -o settings.json \
  plugin_name [other plugin_names]`

2. Modify settings.json according to your testing case.
3. Generate a settings module file (settings.py) for scrapy spider.
  `python -m scrapydd.utils.plugin perform \
  -b your_base_module_import_name \
  -i settings.json \
  -o settings.py
4. Run your scrapydd spider by calling `scrapydd.utils.runner` by passing
  the generated settings module.
"""


import pkg_resources
import json
import sys
import subprocess
import os
import argparse

plugin_env = pkg_resources.Environment([os.path.abspath('./plugins')])


def _pip_installer(requirement):
    print('installing requirement %s' % requirement)
    if isinstance(requirement, str):
        requirement = pkg_resources.Requirement(requirement)
    pargs = [sys.executable, '-m', 'pip', '--disable-pip-version-check',
             'install']
    env = os.environ.copy()
    pargs.append(str(requirement))
    p = subprocess.Popen(pargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         env=env,
                         encoding='UTF8')
    try:
        ret = p.wait(timeout=60)
        output = p.stdout.read()
        err_output = p.stderr.read()
        new_env = pkg_resources.Environment()
        new_env.scan()
        dists = new_env[requirement.name]
        if ret != 0:
            sys.stderr.write('Pip install error\n')
            sys.stderr.write(err_output)
        #dist = pkg_resources.get_distribution(requirement)
        if dists:
            print('%s installed' % dists)
            sys.stdout.flush()
            return dists[0]
        else:
            print('dist not find')
            return None

    except subprocess.TimeoutExpired:
        sys.stderr.write('pip install process timeout.')
        return None


def _activate_distribution(dist):
    pkg_resources.working_set.resolve(dist.requires(),
                                      installer=_pip_installer)
    pkg_resources.working_set.add(dist, replace=True)


def execute(plugin_name):
    try:
        entry_point = list(pkg_resources.iter_entry_points('scrapydd.spliderplugin',
                                                           plugin_name))[0]
    except StopIteration:
        sys.stderr.write('Cannot find plugin %s' % plugin_name)
        return sys.exit(1)
    except IndexError:
        sys.stderr.write('Cannot find plugin %s' % plugin_name)
        return sys.exit(1)
    settings = json.loads(input())
    plugin_execute = entry_point.load()
    output = plugin_execute(settings)
    print(output)


def desc(egg_path):
    try:
        distribution = list(pkg_resources.find_distributions(egg_path))[0]
    except StopIteration:
        raise ValueError("Unknown or corrupt egg")
    except IndexError:
        raise ValueError("Unknown or corrupt egg")

    execute_entry_point = distribution.get_entry_map('scrapydd.spliderplugin')

    if not execute_entry_point:
        sys.stderr.write('Cannot find plugin execute entrypoint')
        return sys.exit(1)

    _activate_distribution(distribution)

    execute_name = next(iter(execute_entry_point))

    plugin_cls = execute_entry_point[execute_name].load()
    plugin = plugin_cls()
    output = plugin.desc()
    print(output)


def _pop_command_name(argv):
    return argv.pop(1)


def generate(plugin_names=None, output_file=None):
    load_plugins()
    output_dict = {}
    for entry_point in pkg_resources.iter_entry_points('scrapydd.spliderplugin'):
        plugin_name = entry_point.name
        if plugin_names and plugin_name not in plugin_names:
            continue
        output_dict[entry_point.name] = {}
        plugin_cls = entry_point.load()
        plugin = plugin_cls()
        plugin_desc = plugin.desc()

        for parameter_key, parameter in plugin_desc['parameters'].items():
            parameter_value = parameter.get('default_value')
            if parameter_value is None:
                parameter_value = ''
            output_dict[entry_point.name][parameter_key] = parameter_value

    output = json.dumps(output_dict, indent=4, sort_keys=True)
    if output_file:
        with open(output_file, 'w') as f:
            f.write(output)
    else:
        print(output)


def load_distribution(egg_path):
    return next(pkg_resources.find_distributions(egg_path, True))


def load_plugins(eggs=None):
    dists, _ = pkg_resources.working_set.find_plugins(plugin_env,
                                                      installer=_pip_installer)

    if eggs:
        for egg in eggs:
            egg_dist = load_distribution(egg)
            dists.append(egg_dist)

    for dist in dists:
        _activate_distribution(dist)


def perform(base_module=None, input_file=None, output_file=None, eggs=None):
    load_plugins(eggs)
    if input_file:
        with open(input_file, 'r') as f:
            settings = json.load(f)
    else:
        settings = json.loads(input())

    if output_file:
        output_stream = open(output_file, 'w')
    else:
        output_stream = sys.stdout

    if base_module:
        output_stream.write('from %s import *' % base_module)

    for plugin_key, plugin_params in settings.items():
        try:
            entry_point = next(pkg_resources.iter_entry_points(
                'scrapydd.spliderplugin', plugin_key))
        except StopIteration:
            raise Exception('Plugin %s not found.' % plugin_key)

        plugin_name = entry_point.name
        plugin_cls = entry_point.load()
        plugin = plugin_cls()
        ops = plugin.execute(plugin_params)

        def format_value(v):
            if isinstance(v, (int, float, bool)):
                return v
            else:
                return '"%s"' % v

        for op in ops:
            if op['op'] == 'set_dict':
                op['value'] = format_value(op['value'])
                output_stream.write('''
try: %(target)s
except NameError: %(target)s = {}
%(target)s['%(key)s'] = %(value)s
''' % op)
            elif op['op'] == 'set_var':
                op['value'] = format_value(op['value'])
                output_stream.write('''
%(var)s = %(value)s
''' % op)
    output_stream.flush()
    if output_file:
        output_stream.close()

def list_():
    load_plugins()
    for entry_point in pkg_resources.iter_entry_points(
                                                'scrapydd.spliderplugin'):
        print(entry_point.name)


def main():
    argv = sys.argv
    cmd = _pop_command_name(argv)
    if cmd == 'execute':
        parser = argparse.ArgumentParser()
        parser.add_argument('-e', '--egg', dest='as_egg',
                            action="store_true", required=False,
                            default=None, help='output json file')
        return execute(sys.argv[1])
    elif cmd == 'desc':
        return desc(sys.argv[1])
    elif cmd == 'generate':
        parser = argparse.ArgumentParser()
        parser.add_argument('-o', '--output', dest='output', required=False,
                            default=None, help='output json file')
        parser.add_argument('plugin_name', nargs='*',
                            help='select plugin to populate settings, '
                            'if no plugin specified, populate all '
                            'plugins installed.')
        args = parser.parse_args()
        return generate(output_file=args.output, plugin_names=args.plugin_name)
    elif cmd == 'perform':
        parser = argparse.ArgumentParser()
        parser.add_argument('-b', '--base', dest='base', required=False,
                            default=None, help='base module to inherit.')
        parser.add_argument('-o', '--output', dest='output', required=False,
                            default=None, help='output module py file. '
                                               'default output to STDOUT')
        parser.add_argument('-i', '--input', dest='input', required=False,
                            default=None, help='input settings json file.'
                                               ' default read from STDIN.')
        parser.add_argument('-e', '--egg', dest='eggs', nargs='*',
                            help='import eggs, can repeat multiple times.')
        args = parser.parse_args()
        return perform(base_module=args.base, input_file=args.input,
                       output_file=args.output, eggs=args.eggs)
    elif cmd == 'list':
        return list_()
    else:
        raise Exception('Not supported cmd %s' % cmd)


if __name__ == '__main__':
    main()
