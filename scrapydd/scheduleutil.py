from optparse import OptionParser, OptionValueError
from scrapy.utils.conf import closest_scrapy_cfg
from six.moves.configparser import ConfigParser, ParsingError
from six.moves.urllib.parse import urlencode, urljoin
from six.moves.urllib.request import urlopen
import urllib
import json

def add_schedule():
    scrapy_cfg = closest_scrapy_cfg()
    cp = ConfigParser()
    if scrapy_cfg:
        cp.read(scrapy_cfg)

    parser = OptionParser(prog  = 'scrapydd add_schedule')
    parser.add_option('-p', '--project', help='the project name')
    parser.add_option('-s', '--spider', help='the spider name')
    parser.add_option('-d', '--schedule', help='cron expression of schedule')
    parser.add_option('--host', help='the server address')
    opts, args = parser.parse_args()

    try:
        project = opts.project or cp.get('deploy', 'project')
    except ParsingError:
        print('Error: project is required')
        parser.print_help()
        return

    spider = opts.spider
    if spider is None:
        print('Error: spider is required')
        parser.print_help()
        return

    try:
        host = opts.host or cp.get('deploy', 'url')
    except Exception:
        print('Error: host is required')
        parser.print_help()
        return

    schedule = opts.schedule
    if schedule is None:
        print('Error: schedule is required')
        parser.print_help()
        return
    url = urljoin(host, '/add_schedule.json')
    postdata = urlencode({
        'project':project,
        'spider':spider,
        'cron':schedule
    })
    res = urlopen(url, postdata)
    print(res.read())
