import tornado.ioloop
import tornado.web
import tornado.template
import os.path
from scrapyd.eggstorage import FilesystemEggStorage
from scrapyd.config import Config
from scrapyd.utils import get_spider_list
from cStringIO import StringIO
from models import Session, Project, Spider, Trigger
from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.triggers.cron import CronTrigger
import json

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

class UploadProject(tornado.web.RequestHandler):
    def post(self):
        egg_storage = FilesystemEggStorage(Config())
        project_name = self.request.arguments['project'][0]
        version = self.request.arguments['version'][0]

        print version
        eggfile = self.request.files['egg'][0]
        eggfilename = eggfile['filename']
        eggf = StringIO(eggfile['body'])
        egg_storage.put(eggf, project_name, version)
        session = Session()
        project = session.query(Project).filter_by(name=project_name).first()
        if project is None:
            project = Project()
            project.name = project_name
            session.add(project)
            session.commit()
            session.refresh(project)
        session.close()
        #eggs_dir = 'eggs'
        #if not os.path.exists(eggs_dir):
        #    os.mkdir(eggs_dir)
        #with open(os.path.join(eggs_dir, eggfilename), 'wb') as f:
        #    f.write(eggfile['body'])
        spiders = get_spider_list(project_name, runner='scrapyd.runner')
        for spider_name in spiders:
            spider = session.query(Spider).filter_by(project_id = project.id, name=spider_name).first()
            if spider is None:
                spider = Spider()
                spider.name = spider_name
                spider.project_id = project.id
                session.add(spider)
                session.commit()

        self.write(eggfilename)
        loader = tornado.template.Loader("scrapymill/templates")
        self.write(loader.load("uploadproject.html").generate(myvalue="XXX"))

    def get(self):
        loader = tornado.template.Loader("scrapymill/templates")
        self.write(loader.load("uploadproject.html").generate(myvalue="XXX"))

class ProjectList(tornado.web.RequestHandler):
    def get(self):
        session = Session()
        projects = session.query(Project)

        response_data = {'projects':{'id': item.id for item in projects}}
        self.write(response_data)
        session.close()

class SpiderInstanceHandler(tornado.web.RequestHandler):
    def get(self, id):
        session = Session()
        spider = session.query(Spider).filter_by(id=id).first()
        loader = tornado.template.Loader("scrapymill/templates")
        self.write(loader.load("spider.html").generate(spider=spider))

class SpiderListHandler(tornado.web.RequestHandler):
    def get(self):
        session = Session()
        spiders = session.query(Spider)
        loader = tornado.template.Loader("scrapymill/templates")
        self.write(loader.load("spiderlist.html").generate(spiders=spiders))
        session.close()


class SpiderTriggersHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler):
        self.scheduler = scheduler

    def get(self, id):
        session = Session()
        spider = session.query(Spider).filter_by(id=id).first()
        loader = tornado.template.Loader("scrapymill/templates")
        self.write(loader.load("spidercreatetrigger.html").generate(spider=spider))

        session.close()

    def post(self, id):
        id = int(id)
        session = Session()
        spider = session.query(Spider).filter_by(id=id).first()
        cron = self.request.arguments['cron'][0]
        cron_parts = cron.split(' ')

        crontrigger = CronTrigger(minute=cron_parts[0],
                                  hour=cron_parts[1],
                                  day=cron_parts[2],
                                  month=cron_parts[3],
                                  day_of_week=cron_parts[4])
        self.scheduler.add_job(func=job_execute, trigger=crontrigger)

        trigger = Trigger()
        trigger.spider_id = spider.id
        trigger.cron_pattern = cron

        session.add(trigger)
        session.commit()
        session.close()
        self.redirect('/spiders/%d'%id)
        #loader = tornado.template.Loader("scrapymill/templates")
        #self.write(loader.load("spidercreatetrigger.html").generate(spider=spider))

def job_execute():
    print 'job trigger fired'



def make_app(scheduler):
    return tornado.web.Application([
        (r"/", MainHandler),
        (r'/uploadproject', UploadProject),
        (r'/projects', ProjectList),
        (r'/spiders', SpiderListHandler),
        (r'/spiders/(\d+)', SpiderInstanceHandler),
        (r'/spiders/(\d+)/triggers', SpiderTriggersHandler, {'scheduler': scheduler}),
    ])

if __name__ == "__main__":
    scheduler = TornadoScheduler()
    app = make_app(scheduler)
    app.listen(8888)

    url = 'sqlite:///database.db'
    scheduler.add_jobstore('sqlalchemy', url=url)
    scheduler.start()

    tornado.ioloop.IOLoop.current().start()