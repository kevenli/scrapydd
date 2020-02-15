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
from argparse import ArgumentParser
from scrapydd.workspace import SpiderSetting
from .runner import main as runner_main
from .plugin import perform, _pip_installer


logger = logging.getLogger(__name__)


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

    spider_setting = SpiderSetting.from_file(args.file)
    plugin_settings = {}
    extra_requirements = spider_setting.extra_requirements
    if extra_requirements:
        for requirement in extra_requirements:
            _pip_installer(requirement)
    with open('plugins.json', 'w') as f:
        json.dump(plugin_settings, f)
    perform(base_module=spider_setting.base_settings_module,
            output_file='settings.py', input_file='plugins.json')
    os.environ['SCRAPY_EXTRA_SETTINGS_MODULE'] = 'settings'
    argv = ['scrapy', 'crawl', spider_setting.spider_name, '-o', 'items.jl']
    for param_key, param_value in spider_setting.spider_parameters.items():
        argv += [
            '-s',
            '%s=%s' % (param_key, param_value)
        ]
    runner_main(argv)


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

