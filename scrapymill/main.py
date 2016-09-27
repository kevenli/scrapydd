import tornado.ioloop
import tornado.web
import tornado.template
import os.path
from scrapyd.eggstorage import FilesystemEggStorage
from scrapyd.config import Config
from scrapyd.utils import get_spider_list
from cStringIO import StringIO

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

class UploadProject(tornado.web.RequestHandler):
    def post(self):
        egg_storage = FilesystemEggStorage(Config())
        project = self.request.arguments['project'][0]
        version = self.request.arguments['version'][0]

        print version
        eggfile = self.request.files['egg'][0]
        eggfilename = eggfile['filename']
        eggf = StringIO(eggfile['body'])
        egg_storage.put(eggf, project, version)
        #eggs_dir = 'eggs'
        #if not os.path.exists(eggs_dir):
        #    os.mkdir(eggs_dir)
        #with open(os.path.join(eggs_dir, eggfilename), 'wb') as f:
        #    f.write(eggfile['body'])
        print get_spider_list(project, runner='scrapyd.runner')
        self.write(eggfilename)
        loader = tornado.template.Loader("templates")
        self.write(loader.load("uploadproject.html").generate(myvalue="XXX"))

    def get(self):
        loader = tornado.template.Loader("templates")
        self.write(loader.load("uploadproject.html").generate(myvalue="XXX"))

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r'/uploadproject', UploadProject),
    ])

if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()