import tornado.ioloop
import tornado.web
import tornado.template
from scrapyd.eggstorage import FilesystemEggStorage
from scrapyd.config import Config
from scrapyd.utils import get_spider_list
from cStringIO import StringIO
from models import Session, Project, Spider, Trigger, SpiderExecutionQueue, Node
from schedule import SchedulerManager
from .nodes import NodeManager
import datetime
import json

def get_template_loader():
    loader = tornado.template.Loader("scrapydd/templates")
    return loader

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
            project.version = version
            session.add(project)
            session.commit()
            session.refresh(project)

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
        loader = get_template_loader()
        self.write(loader.load("uploadproject.html").generate(myvalue="XXX"))
        session.close()

    def get(self):
        loader = get_template_loader()
        self.write(loader.load("uploadproject.html").generate(myvalue="XXX"))


class AddVersionHandler(tornado.web.RequestHandler):
    def post(self):
        egg_storage = FilesystemEggStorage(Config())
        project_name = self.request.arguments['project'][0]
        version = self.request.arguments['version'][0]

        eggfile = self.request.files['egg'][0]
        eggfilename = eggfile['filename']
        eggf = StringIO(eggfile['body'])
        egg_storage.put(eggf, project_name, version)
        session = Session()
        project = session.query(Project).filter_by(name=project_name).first()
        if project is None:
            project = Project()
            project.name = project_name
            project.version = version
            session.add(project)
            session.commit()
            session.refresh(project)

        spiders = get_spider_list(project_name, runner='scrapyd.runner')
        for spider_name in spiders:
            spider = session.query(Spider).filter_by(project_id=project.id, name=spider_name).first()
            if spider is None:
                spider = Spider()
                spider.name = spider_name
                spider.project_id = project.id
                session.add(spider)
                session.commit()

        self.write(json.dumps({'status':'ok', 'spiders':len(spiders)}))
        session.close()


        {"status": "ok", "spiders": 3}

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
        loader = get_template_loader()
        self.write(loader.load("spider.html").generate(spider=spider))
        session.close()

class SpiderEggHandler(tornado.web.RequestHandler):
    def get(self, id):
        session = Session()
        spider = session.query(Spider).filter_by(id=id).first()
        egg_storage = FilesystemEggStorage(Config())
        version, f = egg_storage.get(spider.project.name)
        self.write(f.read())
        session.close()


class SpiderListHandler(tornado.web.RequestHandler):
    def get(self):
        session = Session()
        spiders = session.query(Spider)
        loader = get_template_loader()
        self.write(loader.load("spiderlist.html").generate(spiders=spiders))
        session.close()


class SpiderTriggersHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler_manager):
        self.scheduler_manager = scheduler_manager

    def get(self, id):
        session = Session()
        spider = session.query(Spider).filter_by(id=id).first()
        loader = get_template_loader()
        self.write(loader.load("spidercreatetrigger.html").generate(spider=spider))

        session.close()

    def post(self, id):
        id = int(id)
        session = Session()
        spider = session.query(Spider).filter_by(id=id).first()
        cron = self.request.arguments['cron'][0]
        trigger = Trigger()
        trigger.spider_id = spider.id
        trigger.cron_pattern = cron

        session.add(trigger)
        session.commit()
        session.refresh(trigger)

        self.scheduler_manager.add_job(trigger.id, trigger.cron_pattern)
        session.close()
        self.redirect('/spiders/%d'%id)

class  ExecuteNextHandler(tornado.web.RequestHandler):
    def post(self):
        session = Session()
        node_id = int(self.request.arguments['node_id'][0])
        next_task = session.query(SpiderExecutionQueue).filter_by(status=0).order_by(SpiderExecutionQueue.update_time).first()

        response_data = {'data': None}

        if next_task is not None:
            spider = session.query(Spider).filter_by(id=next_task.spider_id).first()
            project = session.query(Project).filter_by(id=spider.project_id).first()
            next_task.start_time = datetime.datetime.now()
            next_task.update_time = datetime.datetime.now()
            next_task.node_id = node_id
            next_task.status = 1
            session.add(next_task)
            session.commit()
            response_data['data'] = {'task':{
                'task_id': next_task.id,
                'spider_id':  next_task.spider_id,
                'spider_name': next_task.spider_name,
                'project_name': next_task.project_name,
                'version': project.version,
            }}


        self.write(json.dumps(response_data))
        session.close()

class ExecuteCompleteHandler(tornado.web.RequestHandler):
    def post(self):
        session = Session()
        task_id = self.get_argument('task_id')
        log = self.get_argument('log')
        #print log
        session.query(SpiderExecutionQueue).filter_by(id = task_id).delete()
        session.commit()
        session.close()

class NodesHandler(tornado.web.RequestHandler):
    def initialize(self, node_manager):
        self.node_manager = node_manager

    def post(self):
        node = self.node_manager.create_node(self.request.remote_ip)
        self.write(json.dumps({'id': node.id}))


class NodeHeartbeatHandler(tornado.web.RequestHandler):
    def initialize(self, node_manager):
        self.node_manager = node_manager

    def post(self, id):
        session = Session()
        node = session.query(Node).filter_by(id=id).first()
        node.last_heartbeat = datetime.datetime.now()
        session.add(node)
        session.commit()

def make_app(scheduler_manager, node_manager):
    return tornado.web.Application([
        (r"/", MainHandler),
        (r'/uploadproject', UploadProject),
        (r'/addversion.json', AddVersionHandler),
        (r'/projects', ProjectList),
        (r'/spiders', SpiderListHandler),
        (r'/spiders/(\d+)', SpiderInstanceHandler),
        (r'/spiders/(\d+)/egg', SpiderEggHandler),
        (r'/spiders/(\d+)/triggers', SpiderTriggersHandler, {'scheduler_manager': scheduler_manager}),
        (r'/executing/next_task', ExecuteNextHandler),
        (r'/executing/complete', ExecuteCompleteHandler),
        (r'/nodes', NodesHandler, {'node_manager': node_manager}),
        (r'/nodes/(\d+)/heartbeat', NodeHeartbeatHandler, {'node_manager': node_manager}),
    ])

if __name__ == "__main__":
    scheduler_manager = SchedulerManager()
    scheduler_manager.init()

    node_manager = NodeManager(scheduler_manager)
    node_manager.init()

    app = make_app(scheduler_manager, node_manager)
    app.listen(6800)
    tornado.ioloop.IOLoop.current().start()