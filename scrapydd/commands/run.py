from optparse import OptionParser
import sys
from ..utils.runner import main as inproc_run
from ..workspace import VenvRunner, SpiderSetting, DockerRunner
from tornado.ioloop import IOLoop
import os


def run_inproc(cmd, package, settings_path=None):
    os.environ['SCRAPY_EGG'] = package
    if cmd == 'crawl':
        settings = SpiderSetting.from_file(settings_path)
        return inproc_run(['scrapy', 'crawl', settings.spider_name])
    if cmd == 'list':
        return inproc_run(['scrapy', 'list'])


def run_venv(cmd, package_path, settings_path=None):
    with open(package_path, 'rb') as f_package:
        runner = VenvRunner(f_package)
        ioloop = IOLoop.current()
        if cmd == 'crawl':
            settings = SpiderSetting.from_file(settings_path)
            return ioloop.run_sync(lambda: runner.crawl(settings))
        if cmd == 'list':
            spider_list = ioloop.run_sync(runner.list)
            for spider in spider_list:
                print(spider)
            return


def run_docker(cmd, package_path, settings_path=None):
    with open(package_path, 'rb') as f_package:
        runner = DockerRunner(f_package)
        ioloop = IOLoop.current()
        if cmd == 'crawl':
            settings = SpiderSetting.from_file(settings_path)
            return ioloop.run_sync(lambda: runner.crawl(settings))
        if cmd == 'list':
            spider_list = ioloop.run_sync(runner.list)
            for spider in spider_list:
                print(spider)
            return


def main(argv=None):
    if argv is None:
        argv = sys.argv
    parser = OptionParser(usage='scrapydd run {command} [options]',
                          description='run a spider.')
    parser.add_option('-r', '--runner',
                      type='choice',
                      action='store',
                      dest='runner',
                      choices=['inproc', 'venv', 'docker'],
                      default='inproc',
                      help='runner: inproc, venv, docker. [default: inproc]')
    parser.add_option('-p', '--package',
                      action='store',
                      dest='package_file',
                      default='spider.egg',
                      help='specify package file, [default: spider.egg]')
    parser.add_option('-s', '--settings',
                      action='store',
                      dest='settings_file',
                      default='spider.json',
                      help='spider settings file in json format, [default: spider.json]')
    if len(argv) < 2:
        print('Error: no command specified')
        return
    if argv[1] not in ('list', 'crawl'):
        print('Error: wrong command')
        parser.print_help()
        return
    cmd = argv[1]
    opts, args = parser.parse_args(argv)

    if opts.runner == 'inproc':
        return run_inproc(cmd, opts.package_file, opts.settings_file)
    if opts.runner == 'venv':
        return run_venv(cmd, opts.package_file, opts.settings_file)


if __name__ == '__main__':
    main()