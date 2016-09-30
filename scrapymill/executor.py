from tornado.ioloop import IOLoop
import urllib2, urllib
import json
from scrapyd.eggstorage import FilesystemEggStorage
from scrapyd.config import Config
from StringIO import StringIO
import subprocess
import sys
import os


egg_storage = FilesystemEggStorage(Config({'eggs_dir':'executor_eggs'}))

def poll():
    url = 'http://localhost:8888/executing/next_task'
    request= urllib2.Request(url=url,data='')
    res = urllib2.urlopen(request)
    response_data = json.loads(res.read())
    if 'spider_id' in response_data:
        task_id = response_data['task_id']
        spider_id = response_data['spider_id']
        project_name = response_data['project_name']
        spider_name = response_data['spider_name']

        egg_request_url = 'http://localhost:8888/spiders/%d/egg' % spider_id
        print egg_request_url
        egg_request=urllib2.Request(egg_request_url)
        res = urllib2.urlopen(request)
        egg = StringIO(res.read())

        egg_storage.put(egg, project_name, '1')

        runner = 'scrapyd.runner'
        pargs = [sys.executable, '-m', runner, 'crawl', spider_name]
        env = os.environ.copy()
        env['SCRAPY_PROJECT'] = str(project_name)
        subprocess.call(pargs, env=env)

        task_complete_url = 'http://localhost:8888/executing/complete'
        task_complete_postdata = urllib.urlencode({'task_id':task_id})
        task_complete_request = urllib2.Request(task_complete_url, data=task_complete_postdata)
        urllib2.urlopen(task_complete_request).read()

    ioloop = IOLoop.current()
    ioloop.call_later(1, poll)






if __name__ == '__main__':
    ioloop = IOLoop.current()
    ioloop.call_later(delay=1, callback=poll)
    ioloop.start()
