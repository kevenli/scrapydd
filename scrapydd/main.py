import tornado.ioloop
import tornado.web
import tornado.template
from scrapyd.eggstorage import FilesystemEggStorage
import scrapyd.config
from scrapyd.utils import get_spider_list
from cStringIO import StringIO
from models import Session, Project, Spider, Trigger, SpiderExecutionQueue, Node, init_database
from schedule import SchedulerManager
from .nodes import NodeManager
import datetime
import json
from .config import Config
import os.path
import sys
import logging
from .exceptions import *


def get_template_loader():
    loader = tornado.template.Loader(os.path.join(os.path.dirname(__file__), "templates"))
    return loader

def get_egg_storage():
    return FilesystemEggStorage(scrapyd.config.Config())

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

class UploadProject(tornado.web.RequestHandler):
    def post(self):
        egg_storage = get_egg_storage()
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
            spider = session.query(Spider).filter_by(project_id = project.id, name=spider_name).first()
            if spider is None:
                spider = Spider()
                spider.name = spider_name
                spider.project_id = project.id
                session.add(spider)
                session.commit()
        if self.request.path.endswith('.json'):
            self.write(json.dumps({'status': 'ok', 'spiders': len(spiders)}))
        else:
            loader = get_template_loader()
            self.write(loader.load("uploadproject.html").generate(myvalue="XXX"))
        session.close()

    def get(self):
        loader = get_template_loader()
        self.write(loader.load("uploadproject.html").generate(myvalue="XXX"))

class ScheduleHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler_manager):
        self.scheduler_manager = scheduler_manager

    def post(self):
        project = self.get_argument('project')
        spider = self.get_argument('spider')

        try:
            jobid = self.scheduler_manager.add_task(project, spider)
            response_data = {
                'status':'ok',
                'jobid': jobid
            }
            self.write(json.dumps(response_data))
        except JobRunning as e:
            response_data = {
                'status':'error',
                'errormsg': 'job is running with jobid %s' % e.jobid
            }
            self.set_status(400, 'job is running')
            self.write(json.dumps(response_data))

class AddScheduleHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler_manager):
        self.scheduler_manager = scheduler_manager

    def post(self):
        project = self.get_argument('project')
        spider = self.get_argument('spider')
        cron = self.get_argument('cron')
        try:
            self.scheduler_manager.add_schedule(project, spider, cron)
            response_data = {
                'status':'ok',
            }
            self.write(json.dumps(response_data))
        except SpiderNotFound:
            response_data = {
                'status':'error',
                'errormsg': 'spider not found',
            }
            self.write(json.dumps(response_data))
        except ProjectNotFound:
            response_data = {
                'status':'error',
                'errormsg': 'project not found',
            }
            self.write(json.dumps(response_data))
        except InvalidCronExpression:
            response_data = {
                'status':'error',
                'errormsg': 'invalid cron expression.',
            }
            self.write(json.dumps(response_data))




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

class SpiderInstanceHandler2(tornado.web.RequestHandler):
    def get(self, project, spider):
        session = Session()
        project = session.query(Project).filter(Project.name == project).first()
        spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider).first()
        loader = get_template_loader()
        self.write(loader.load("spider.html").generate(spider=spider))
        session.close()

class SpiderEggHandler(tornado.web.RequestHandler):
    def get(self, id):
        session = Session()
        spider = session.query(Spider).filter_by(id=id).first()
        egg_storage = get_egg_storage()
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

    def get(self, project, spider):
        session = Session()
        project = session.query(Project).filter(Project.name == project).first()
        spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider).first()
        loader = get_template_loader()
        self.write(loader.load("spidercreatetrigger.html").generate(spider=spider))

        session.close()

    def post(self, project, spider):
        cron = self.get_argument('cron')
        self.scheduler_manager.add_schedule(project, spider, cron)
        self.redirect('/projects/%s/spiders/%s'% (project, spider))

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
        job = session.query(SpiderExecutionQueue).filter_by(id = task_id).first()
        spider_log_folder = os.path.join('logs', job.project_name, job.spider_name)
        if not os.path.exists(spider_log_folder):
            os.makedirs(spider_log_folder)
        log_file = os.path.join(spider_log_folder, job.id + '.log')

        with open(log_file, 'w') as f:
            f.write(log)
        job.status = 2
        job.update_time = datetime.datetime.now()
        session.add(job)
        session.commit()
        session.close()
        logging.info('Job %s completed.' % task_id)
        response_data = {'status': 'ok'}
        self.write(json.dumps(response_data))

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
        node_id = int(id)
        try:
            self.node_manager.heartbeat(node_id)
            response_data = {'status':'ok'}
        except NodeExpired:
            response_data = {'status': 'error', 'errmsg': 'Node expired'}
            self.set_status(400, 'Node expired')
        self.write(json.dumps(response_data))

class JobsHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler_manager):
        self.scheduler_manager = scheduler_manager

    def get(self):
        pending, running, finished = self.scheduler_manager.jobs()
        context = {
            'pending': pending,
            'running': running,
            'finished': finished,
        }
        loader = get_template_loader()
        self.write(loader.load("jobs.html").generate(**context))

class LogsHandler(tornado.web.RequestHandler):
    def get(self, project, spider, jobid):
        logfilepath = os.path.join('logs', project, spider, jobid + '.log')
        with open(logfilepath, 'r') as f:
        #    self.write(f.read())
            log = f.read()
        loader = get_template_loader()
        self.write(loader.load("log.html").generate(log=log))

class JobStartHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler_manager):
        self.scheduler_manager = scheduler_manager

    def post(self, jobid):
        pid = self.get_argument('pid')
        self.scheduler_manager.job_start(jobid, pid)


def make_app(scheduler_manager, node_manager):
    return tornado.web.Application([
        (r"/", MainHandler),
        (r'/uploadproject', UploadProject),
        (r'/addversion.json', UploadProject),
        (r'/schedule.json', ScheduleHandler, {'scheduler_manager': scheduler_manager}),
        (r'/add_schedule.json', AddScheduleHandler, {'scheduler_manager': scheduler_manager}),
        (r'/projects', ProjectList),
        (r'/spiders', SpiderListHandler),
        (r'/spiders/(\d+)', SpiderInstanceHandler),
        (r'/spiders/(\d+)/egg', SpiderEggHandler),
        (r'/projects/(\w+)/spiders/(\w+)', SpiderInstanceHandler2),
        (r'/projects/(\w+)/spiders/(\w+)/triggers', SpiderTriggersHandler, {'scheduler_manager': scheduler_manager}),
        (r'/executing/next_task', ExecuteNextHandler),
        (r'/executing/complete', ExecuteCompleteHandler),
        (r'/nodes', NodesHandler, {'node_manager': node_manager}),
        (r'/nodes/(\d+)/heartbeat', NodeHeartbeatHandler, {'node_manager': node_manager}),
        (r'/jobs', JobsHandler, {'scheduler_manager': scheduler_manager}),
        (r'/jobs/(\w+)/start', JobStartHandler, {'scheduler_manager': scheduler_manager}),
        (r'/logs/(\w+)/(\w+)/(\w+).log', LogsHandler),
    ])

def run(argv=None):
    config = Config()
    if config.getboolean('debug'):
        logging.basicConfig(level=logging.DEBUG)
    if argv is None:
        argv = sys.argv
    logging.debug('starting server with argv : %s' % str(argv))

    init_database()

    scheduler_manager = SchedulerManager()
    scheduler_manager.init()

    node_manager = NodeManager(scheduler_manager)
    node_manager.init()

    app = make_app(scheduler_manager, node_manager)

    bind_address = config.get('bind_address')
    bind_port = config.getint('bind_port')
    print 'Starting server on %s:%d' % (bind_address, bind_port)
    app.listen(bind_port, bind_address)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    run()