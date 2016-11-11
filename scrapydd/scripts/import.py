import os
from scrapyd.eggstorage import FilesystemEggStorage
from scrapyd.config import Config
import urllib2
from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
register_openers()

source_dir = '/kf/scrapyd'

dest_url = 'http://localhost:6801/addversion.json'

source_eggs_dir = os.path.join(source_dir, 'eggs')
source_config = Config({'eggs_dir': source_eggs_dir})
source_egg_storage = FilesystemEggStorage(source_config)
for dir in os.listdir(source_eggs_dir):
    #print dir
    project = dir
    version, egg = source_egg_storage.get(project)
    print project, version
    post_data = {
        'egg': egg,
        'project': project,
        'version': version,
    }
    datagen, headers = multipart_encode(post_data)
    request = urllib2.Request(url=dest_url, headers=headers, data=datagen)
    try:
        res = urllib2.urlopen(request)
    except urllib2.HTTPError as e:
        print 'HTTPError: %s' % e
        print e.read()
        break

    
