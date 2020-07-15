"""
    The runner2 module is designed to run spider in one command.
    To archive this all necessary information should be put into files.
    These files contains:
        * The spider package file. (spider.egg)
        * The spider setting file. (spider.json)
        * (Optional) Plugin packages. (`plugins/xxx.egg`)

    This module can also resolve uninstalled dependencies installation.
"""

import os
import logging
import json
import yaml
import string
import random
import tempfile
import sys
import shutil
from argparse import ArgumentParser
from scrapydd.workspace import SpiderSetting
from .runner import main as runner_main
from .plugin import perform, _pip_installer


logger = logging.getLogger(__name__)


def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def main():
    """
      Need put plugin packages(eggs) in the `plugin` folder first.
    :return:
    """
    parser = ArgumentParser()
    parser.add_argument('-f', '--file', dest='file', required=False,
                        default='spider.json', help='The spider settings json '
                                                    'file')
    args = parser.parse_args()
    file_ext = os.path.splitext(args.file)[1]
    if file_ext.lower() in ('.yaml', '.yml'):
        with open(args.file, 'r') as f:
            dic = yaml.load(f, yaml.Loader)
    elif file_ext.lower() == '.json':
        with open(args.file, 'r') as f:
            dic = json.load(f)
    else:
        raise Exception(f'Not supported file type : {args.file}')

    spider_setting = SpiderSetting.from_dict(dic)
    plugin_settings = spider_setting.plugin_settings
    extra_requirements = spider_setting.extra_requirements
    if extra_requirements:
        for requirement in extra_requirements:
            _pip_installer(requirement)
    try:
        settings_module = 'settings_' + randomString(6)
        settings_package = tempfile.mkdtemp()

        settings_stream = open(os.path.join(settings_package,
                                            settings_module+'.py'), 'w')
        if plugin_settings:
            perform(base_module=spider_setting.base_settings_module,
                    output_file=settings_stream, input_file=plugin_settings)
        settings_stream.close()
        sys.path.append(settings_package)
        os.environ['SCRAPY_EXTRA_SETTINGS_MODULE'] = settings_module
        output_file = spider_setting.output_file or 'items.jl'
        argv = ['scrapy', 'crawl', spider_setting.spider_name, '-o', output_file]
        for param_key, param_value in spider_setting.spider_parameters.items():
            argv += [
                '-s',
                '%s=%s' % (param_key, param_value)
            ]
        runner_main(argv)
    except SystemExit:
        pass
    finally:
        if os.path.exists(settings_package):
            shutil.rmtree(settings_package)

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

