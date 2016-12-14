# -*-coding:utf8-*-
import tornado.ioloop
import tornado.web
import tornado.template
from scrapyd.eggstorage import FilesystemEggStorage
import scrapyd.config
from cStringIO import StringIO
from models import Session, Project, Spider, Trigger, SpiderExecutionQueue, Node, init_database, HistoricalJob, \
    SpiderWebhook, session_scope, SpiderSettings
from schedule import SchedulerManager
from scrapydd.nodes import NodeManager
import datetime
import json
from scrapydd.config import Config
from scrapydd.process import fork_processes
import os.path
import sys
import logging
from .exceptions import *
from sqlalchemy import desc
from optparse import OptionParser, OptionValueError
import subprocess
import signal
from stream import PostDataStreamer
from webhook import WebhookDaemon
from daemonize import daemonize
from tornado import gen
import tornado.httpserver
import tornado.netutil
from workspace import ProjectWorkspace
from scrapydd.cluster import ClusterNode
from scrapydd.ssl_gen import SSLCertificateGenerator
import ssl

logger = logging.getLogger(__name__)


def get_template_loader():
    loader = tornado.template.Loader(os.path.join(os.path.dirname(__file__), "templates"))
    return loader


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        session = Session()
        projects = list(session.query(Project).order_by(Project.name))
        loader = get_template_loader()
        self.write(loader.load("index.html").generate(projects=projects))
        session.close()


class UploadProject(tornado.web.RequestHandler):
    @gen.coroutine
    def post(self):
        project_name = self.request.arguments['project'][0]
        version = self.request.arguments['version'][0]
        eggfile = self.request.files['egg'][0]
        eggf = StringIO(eggfile['body'])

        try:
            workspace = ProjectWorkspace(project_name)
            yield workspace.init()
            spiders = yield workspace.test_egg(eggf)
            workspace.put_egg(eggf, version)

        except InvalidProjectEgg as e:
            logger.error('Error when uploading project, %s %s' % (e.message, e.detail))
            self.set_status(400, reason=e.message)
            self.finish("<html><title>%(code)d: %(message)s</title>"
                            "<body><pre>%(output)s</pre></body></html>" % {
                                "code": 400,
                                "message": e.message,
                                "output": e.detail,
                            })
            return
        except ProcessFailed as e:
            logger.error('Error when uploading project, %s, %s' % (e.message, e.std_output))
            self.set_status(400, reason=e.message)
            self.finish("<html><title>%(code)d: %(message)s</title>"
                            "<body><pre>%(output)s</pre></body></html>" % {
                                "code": 400,
                                "message": e.message,
                                "output": e.std_output,
                            })
            return
        finally:
            workspace.clearup()

        logger.debug('spiders: %s' % spiders)
        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()
            if project is None:
                project = Project()
                project.name = project_name
            project.version = version
            session.add(project)
            session.commit()
            session.refresh(project)

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
                self.write(loader.load("uploadproject.html").generate())

    def get(self):
        loader = get_template_loader()
        self.write(loader.load("uploadproject.html").generate())

class ScheduleHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler_manager):
        self.scheduler_manager = scheduler_manager

    def post(self):
        project = self.get_argument('project')
        spider = self.get_argument('spider')

        try:
            job = self.scheduler_manager.add_task(project, spider)
            jobid = job.id
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
        jobs = session.query(HistoricalJob)\
            .filter(HistoricalJob.spider_id == spider.id)\
            .order_by(desc(HistoricalJob.start_time))\
            .slice(0, 100)
        running_jobs = session.query(SpiderExecutionQueue)\
            .filter(SpiderExecutionQueue.spider_id == spider.id)\
            .order_by(desc(SpiderExecutionQueue.update_time))

        webhook = session.query(SpiderWebhook).filter_by(id=spider.id).first()
        context = {}
        context['spider'] = spider
        context['project'] = project
        context['jobs'] = jobs
        context['webhook'] = webhook
        context['running_jobs'] = running_jobs
        context['settings'] = session.query(SpiderSettings).filter_by(spider_id = spider.id).order_by(SpiderSettings.setting_key)
        loader = get_template_loader()
        self.write(loader.load("spider.html").generate(**context))
        session.close()

class SpiderEggHandler(tornado.web.RequestHandler):
    def get(self, id):
        session = Session()
        spider = session.query(Spider).filter_by(id=id).first()
        version, f = ProjectWorkspace(spider.project.name).get_egg()
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
        with session_scope() as session:
            project = session.query(Project).filter(Project.name == project).first()
            spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider).first()
            loader = get_template_loader()
            context = {}
            context['spider'] = spider
            context['errormsg'] = None
            self.write(loader.load("spidercreatetrigger.html").generate(**context))

    def post(self, project, spider):
        cron = self.get_argument('cron')
        with session_scope() as session:
            project = session.query(Project).filter(Project.name == project).first()
            spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider).first()
            try:
                self.scheduler_manager.add_schedule(project, spider, cron)
                return self.redirect('/projects/%s/spiders/%s' % (project.name, spider.name))
            except InvalidCronExpression:
                loader = get_template_loader()
                context = {}
                context['spider'] = spider
                context['errormsg'] = 'Invalid cron expression '
                return self.write(loader.load("spidercreatetrigger.html").generate(**context))


class DeleteSpiderTriggersHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler_manager):
        self.scheduler_manager = scheduler_manager

    def post(self, project_name, spider_name, trigger_id):
        self.scheduler_manager.remove_schedule(project_name, spider_name, trigger_id)
        self.redirect('/projects/%s/spiders/%s' % (project_name, spider_name))

class  ExecuteNextHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler_manager):
        self.scheduler_manager = scheduler_manager

    def post(self):
        with session_scope() as session:
            node_id = int(self.request.arguments['node_id'][0])
            next_task = self.scheduler_manager.get_next_task(node_id)

            response_data = {'data': None}

            if next_task is not None:
                spider = session.query(Spider).filter_by(id=next_task.spider_id).first()
                if not spider:
                    logger.error('Task %s has not spider, deleting.' % next_task.id)
                    session.query(SpiderExecutionQueue).filter_by(id=next_task.id).delete()
                    self.write({'data': None})
                    return

                project = session.query(Project).filter_by(id=spider.project_id).first()
                if not project:
                    logger.error('Task %s has not project, deleting.' % next_task.id)
                    session.query(SpiderExecutionQueue).filter_by(id=next_task.id).delete()
                    self.write({'data': None})
                    return
                response_data['data'] = {'task':{
                    'task_id': next_task.id,
                    'spider_id':  next_task.spider_id,
                    'spider_name': next_task.spider_name,
                    'project_name': next_task.project_name,
                    'version': project.version,
                }}
            self.write(json.dumps(response_data))


@tornado.web.stream_request_body
class ExecuteCompleteHandler(tornado.web.RequestHandler):
    def initialize(self, webhook_daemon, scheduler_manager):
        '''

        @type webhook_daemon : WebhookDaemon
        :return:
        '''
        self.webhook_daemon = webhook_daemon
        self.scheduler_manager = scheduler_manager

    #
    def prepare(self):
        MB = 1024 * 1024
        GB = 1024 * MB
        TB = 1024 * GB
        MAX_STREAMED_SIZE = 1 * GB
        # set the max size limiation here
        self.request.connection.set_max_body_size(MAX_STREAMED_SIZE)
        try:
            total = int(self.request.headers.get("Content-Length", "0"))
        except:
            total = 0
        self.ps = PostDataStreamer(total)  # ,tmpdir="/tmp"

        # self.fout = open("raw_received.dat","wb+")

    def post(self):
        try:
            self.ps.finish_receive()
            fields = self.ps.get_values(['task_id','status'])
            logger.debug(self.ps.get_nonfile_names())
            node_id = self.request.headers.get('X-Dd-Nodeid')
            task_id = fields['task_id']
            status = fields['status']
            if status == 'success':
                status_int = 2
            elif status == 'fail':
                status_int = 3
            else:
                self.set_status(401, 'Invalid argument: status.')
                return

            session = Session()
            query = session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.id == task_id, SpiderExecutionQueue.status == 1)
            # be compatible with old agent version
            if node_id:
                query = query.filter(SpiderExecutionQueue.node_id == node_id)
            else:
                logger.warning('Agent has not specified node id in complete request, client address: %s.' % self.request.remote_ip)
            job = query.first()

            if job is None:
                self.set_status(404, 'Job not found.')
                session.close()
                return
            log_file = None
            items_file = None
            try:
                spider_log_folder = os.path.join('logs', job.project_name, job.spider_name)
                if not os.path.exists(spider_log_folder):
                    os.makedirs(spider_log_folder)

                log_part = self.ps.get_parts_by_name('log')[0]
                if log_part:
                    import shutil
                    log_file = os.path.join(spider_log_folder, job.id + '.log')
                    shutil.copy(log_part['tmpfile'].name, log_file)

            except Exception as e:
                logger.error('Error when writing task log file, %s' % e)

            items_parts = self.ps.get_parts_by_name('items')
            if items_parts:
                try:
                    part = items_parts[0]
                    tmpfile = part['tmpfile'].name
                    logger.debug('tmpfile size: %d' % os.path.getsize(tmpfile))
                    items_file_path = os.path.join('items', job.project_name, job.spider_name)
                    if not os.path.exists(items_file_path):
                        os.makedirs(items_file_path)
                    items_file = os.path.join(items_file_path, '%s.jl' % job.id)
                    import shutil
                    shutil.copy(tmpfile, items_file)
                    logger.debug('item file size: %d' % os.path.getsize(items_file))
                except Exception as e:
                    logger.error('Error when writing items file, %s' % e)

            job.status = status_int
            job.update_time = datetime.datetime.now()
            historical_job = self.scheduler_manager.job_finished(job, log_file, items_file)
            if items_file:
                self.webhook_daemon.on_spider_complete(historical_job, items_file)

            session.close()
            logger.info('Job %s completed.' % task_id)
            response_data = {'status': 'ok'}
            self.write(json.dumps(response_data))


        finally:
            # Don't forget to release temporary files.
            self.ps.release_parts()


    def data_received(self, chunk):
        # self.fout.write(chunk)
        self.ps.receive(chunk)


class NodesHandler(tornado.web.RequestHandler):
    def initialize(self, node_manager):
        self.node_manager = node_manager

    def post(self):
        node = self.node_manager.create_node(self.request.remote_ip)
        self.write(json.dumps({'id': node.id}))


class NodeHeartbeatHandler(tornado.web.RequestHandler):
    def initialize(self, node_manager, scheduler_manager):
        self.node_manager = node_manager
        self.scheduler_manager = scheduler_manager

    def post(self, id):
        #logger.debug(self.request.headers)
        node_id = int(id)
        self.set_header('X-DD-New-Task', self.scheduler_manager.has_task())
        try:
            self.node_manager.heartbeat(node_id)
            running_jobs = self.request.headers.get('X-DD-RunningJobs', None)
            if running_jobs:
                self.scheduler_manager.jobs_running(node_id, running_jobs.split(','))
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
        with session_scope() as session:
            job = session.query(HistoricalJob).filter_by(id=jobid).first()
            log_file = job.log_file
            with open(log_file, 'r') as f:
                log = f.read()
            loader = get_template_loader()
            self.write(loader.load("log.html").generate(log=log))


class ItemsFileHandler(tornado.web.RequestHandler):
    def get(self, project, spider, jobid):
        with session_scope() as session:
            job = session.query(HistoricalJob).filter_by(id=jobid).first()
            items_file =job.items_file
            self.set_header('Content-Type', 'application/json')
            self.write(open(items_file, 'r').read())


class JobStartHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler_manager):
        self.scheduler_manager = scheduler_manager

    def post(self, jobid):
        pid = self.get_argument('pid')
        pid = int(pid) if pid else None
        self.scheduler_manager.job_start(jobid, pid)


class SpiderWebhookHandler(tornado.web.RequestHandler):
    def get(self, project_name, spider_name):
        session = Session()
        project = session.query(Project).filter(Project.name == project_name).first()
        spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider_name).first()

        webhook = session.query(SpiderWebhook).filter_by(id=spider.id).first()
        self.write(webhook.payload_url)

    def post(self, project_name, spider_name):
        payload_url = self.get_argument('payload_url')
        session = Session()
        project = session.query(Project).filter(Project.name == project_name).first()
        spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider_name).first()

        webhook = session.query(SpiderWebhook).filter_by(id=spider.id).first()
        if webhook is None:
            webhook = SpiderWebhook()
            webhook.payload_url = payload_url
            webhook.id = spider.id
        else:
            webhook.payload_url = payload_url
        session.add(webhook)
        session.commit()
        session.close()

    def put(self, project_name, spider_name):
        self.post(project_name, spider_name)

    def delete(self, project_name, spider_name):
        session = Session()
        project = session.query(Project).filter(Project.name == project_name).first()
        spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider_name).first()
        session.query(SpiderWebhook).filter_by(id=spider.id).delete()
        session.commit()
        session.close()

class DeleteProjectHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler_manager):
        self.scheduler_manager = scheduler_manager

    def post(self):
        project_name = self.get_argument('project')
        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()
            spiders = session.query(Spider).filter_by(project_id=project.id)
            for spider in spiders:
                triggers = session.query(Trigger).filter_by(spider_id = spider.id)
                session.query(SpiderExecutionQueue).filter_by(spider_id = spider.id).delete()
                session.commit()
                for trigger in triggers:
                    self.scheduler_manager.remove_schedule(project_name, spider.name, trigger_id=trigger.id)
                for history_log in session.query(HistoricalJob).filter(HistoricalJob.spider_id == spider.id):
                    try:
                        os.remove(history_log.log_file)
                    except:
                        pass
                    try:
                        os.remove(history_log.items_file)
                    except:
                        pass
                    session.delete(history_log)
                session.delete(spider)
            session.delete(project)
            ProjectWorkspace(project_name).delete_egg(project_name)


class SpiderSettingsHandler(tornado.web.RequestHandler):
    def get(self, project, spider):
        with session_scope() as session:
            project = session.query(Project).filter_by(name=project).first()
            spider = session.query(Spider).filter_by(project_id = project.id, name=spider).first()
            #settings = session.query(SpiderSettings).filter_by(spider_id = spider.id)
            concurrency_setting = session.query(SpiderSettings).filter_by(spider_id = spider.id, setting_key='concurrency').first()
            if not concurrency_setting:
                concurrency_setting = SpiderSettings()
                concurrency_setting.setting_key = 'concurrency_setting'
                concurrency_setting.value = 1
            template = get_template_loader().load('spidersettings.html')
            context = {}
            context['settings'] = {'concurrency': concurrency_setting}
            context['project'] = project
            context['spider'] = spider

            return self.write(template.generate(**context))

    def post(self, project, spider):
        concurrency = int(self.get_argument('concurrency'))
        with session_scope() as session:
            project = session.query(Project).filter_by(name=project).first()
            spider = session.query(Spider).filter_by(project_id = project.id, name=spider).first()

            concurrency_setting = session.query(SpiderSettings).filter_by(spider_id = spider.id, setting_key='concurrency').first()
            if not concurrency_setting:
                concurrency_setting = SpiderSettings()
                concurrency_setting.spider_id = spider.id
                concurrency_setting.setting_key = 'concurrency'
            concurrency_setting.value = concurrency
            session.add(concurrency_setting)
            self.redirect('/projects/%s/spiders/%s' % (project.name, spider.name))


class CACertHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(open('keys/ca.crt','rb').read())
        self.set_header('Content-Type', 'application/cert')


class ListProjectVersionsHandler(tornado.web.RequestHandler):
    def get(self):
        try:
            project = self.get_argument('project')
        except tornado.web.MissingArgumentError as e:
            return self.write({'status': 'error', 'message': e.arg_name})

        workspace = ProjectWorkspace(project)
        versions = workspace.list_versions(project)
        return self.write({'status':'ok', 'versions': versions})


def make_app(scheduler_manager, node_manager, webhook_daemon):
    '''

    @type scheduler_manager SchedulerManager
    @type node_manager NodeManager
    @type webhook_daemon: WebhookDaemon

    :return:
    '''
    return tornado.web.Application([
        (r"/", MainHandler),
        (r'/uploadproject', UploadProject),
        (r'/addversion.json', UploadProject),
        (r'/delproject.json', DeleteProjectHandler, {'scheduler_manager': scheduler_manager}),
        (r'/listversions.json', ListProjectVersionsHandler),
        (r'/schedule.json', ScheduleHandler, {'scheduler_manager': scheduler_manager}),
        (r'/add_schedule.json', AddScheduleHandler, {'scheduler_manager': scheduler_manager}),
        (r'/projects', ProjectList),
        (r'/spiders', SpiderListHandler),
        (r'/spiders/(\d+)', SpiderInstanceHandler),
        (r'/spiders/(\d+)/egg', SpiderEggHandler),
        (r'/projects/(\w+)/spiders/(\w+)', SpiderInstanceHandler2),
        (r'/projects/(\w+)/spiders/(\w+)/triggers', SpiderTriggersHandler, {'scheduler_manager': scheduler_manager}),
        (r'/projects/(\w+)/spiders/(\w+)/triggers/(\w+)/delete', DeleteSpiderTriggersHandler, {'scheduler_manager': scheduler_manager}),
        (r'/projects/(\w+)/spiders/(\w+)/settings', SpiderSettingsHandler),
        (r'/projects/(\w+)/spiders/(\w+)/webhook', SpiderWebhookHandler),
        (r'/executing/next_task', ExecuteNextHandler, {'scheduler_manager': scheduler_manager}),
        (r'/executing/complete', ExecuteCompleteHandler, {'webhook_daemon': webhook_daemon, 'scheduler_manager': scheduler_manager}),
        (r'/nodes', NodesHandler, {'node_manager': node_manager}),
        (r'/nodes/(\d+)/heartbeat', NodeHeartbeatHandler, {'node_manager': node_manager, 'scheduler_manager': scheduler_manager}),
        (r'/jobs', JobsHandler, {'scheduler_manager': scheduler_manager}),
        (r'/jobs/(\w+)/start', JobStartHandler, {'scheduler_manager': scheduler_manager}),
        (r'/logs/(\w+)/(\w+)/(\w+).log', LogsHandler),
        (r'/items/(\w+)/(\w+)/(\w+).jl', ItemsFileHandler),
        (r'/ca.crt', CACertHandler),
    ])

def check_and_gen_ssl_keys(config):
    server_name = config.get('server_name')
    if not server_name:
        raise Exception('Must specify a server name')
    ssl_gen = SSLCertificateGenerator()
    try:
        ca_key = ssl_gen.get_ca_key()
        ca_cert = ssl_gen.get_ca_cert()
    except IOError:
        logger.info('ca cert not exist, creating new cert and key.')
        ssl_gen.gen_ca('scrapydd', 'scrapydd')

    host_name = config.get('server_name')
    if not os.path.exists(os.path.join('keys', '%s.crt' % host_name)):
        logger.info('server cert not exist, creating new.')
        alt_names = [s.strip() for s in config.get('dns_alt_names').split(',')]
        if '' in alt_names:
            alt_names.remove('')
        ssl_gen.gen_cert(host_name, usage=2, alt_names=alt_names)

def start_server(argv=None):
    config = Config()
    logging.debug('starting server with argv : %s' % str(argv))

    init_database()
    bind_address = config.get('bind_address')
    bind_port = config.getint('bind_port')
    try:
        https_port = int(config.get('https_port')) if config.get('https_port') else None
    except ValueError:
        logger.warning('https_port is configured, but it is not int, %s' % config.get('https_port'))
        https_port = None

    if https_port:
        https_sockets = tornado.netutil.bind_sockets(https_port, bind_address)

    sockets = tornado.netutil.bind_sockets(bind_port, bind_address)

    # task_id is current process identifier when forked processes, start with 0
    task_id = None
    if not sys.platform.startswith('win'):
        task_id = fork_processes(config.getint('fork_proc_count'))
    else:
        logger.warning('Windows platform does not support forking process, running in single process mode.')

    cluster_sync_obj = None
    if task_id is not None and config.get('cluster_bind_address'):
        cluster_node = ClusterNode(task_id, config)
        cluster_sync_obj = cluster_node.sync_obj

    scheduler_manager = SchedulerManager(config=config, syncobj=cluster_sync_obj)
    scheduler_manager.init()

    node_manager = NodeManager(scheduler_manager)
    node_manager.init()

    webhook_daemon = WebhookDaemon()
    webhook_daemon.init()

    app = make_app(scheduler_manager, node_manager, webhook_daemon)


    server = tornado.httpserver.HTTPServer(app)
    server.add_sockets(sockets)

    if https_port:
        check_and_gen_ssl_keys(config)
        if config.getboolean('client_validation'):
            ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        else:
            ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.load_cert_chain(os.path.join('keys', "%s.crt" % config.get('server_name')),
                                os.path.join('keys', "%s.key" % config.get('server_name')))
        ssl_ctx.load_verify_locations(cafile='keys/ca.crt')
        ssl_ctx.check_hostname = False
        httpsserver = tornado.httpserver.HTTPServer(app, ssl_options=ssl_ctx)
        httpsserver.add_sockets(https_sockets)
        logger.info('starting https server on %s:%s' % (bind_address, https_port))
    ioloop = tornado.ioloop.IOLoop.current()
    ioloop.start()

def run(argv=None):
    if argv is None:
        argv = sys.argv
    parser = OptionParser(prog  = 'scrapydd server')
    parser.add_option('--daemon', action='store_true', help='run scrapydd server in daemon mode')
    parser.add_option('--pidfile', help='pid file will be created when daemon started')
    opts, args = parser.parse_args(argv)
    pidfile = opts.pidfile or 'scrapydd-server.pid'

    config = Config()
    init_logging(config)

    if opts.daemon:
        print 'starting daemon.'
        daemon = Daemon(pidfile=pidfile)
        daemon.start()
        sys.exit(0)
    else:
        start_server()

def init_logging(config):
    import logging.handlers
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logger = logging.getLogger()
    fh = logging.handlers.TimedRotatingFileHandler(os.path.join(log_dir, 'scrapydd-server.log'), when='D', backupCount=7)
    eh = logging.handlers.TimedRotatingFileHandler(os.path.join(log_dir, 'scrapydd-error.log'), when='D', backupCount=30)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    eh.setFormatter(formatter)
    eh.setLevel(logging.ERROR)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.addHandler(eh)

    if config.getboolean('debug'):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    access_log_logger = logging.getLogger('tornado.access')
    access_log_fh = logging.handlers.TimedRotatingFileHandler(os.path.join(log_dir, 'scrapydd-access.log'), when='D', backupCount=30)
    access_log_logger.addHandler(access_log_fh)
    access_log_logger.setLevel(logging.INFO)


class Daemon():
    def __init__(self, pidfile):
        self.pidfile = pidfile
        self.subprocess_p = None
        self.pid = 0

    def start_subprocess(self):
        argv = sys.argv
        argv.remove('--daemon')
        pargs = argv
        env = os.environ.copy()
        self.subprocess_p = subprocess.Popen(pargs, env=env)
        signal.signal(signal.SIGINT, self.on_signal)
        signal.signal(signal.SIGTERM, self.on_signal)
        self.subprocess_p.wait()

    def read_pidfile(self):
        try:
            with open(self.pidfile, 'r') as f:
                return int(f.readline())
        except IOError:
            return None

    def try_remove_pidfile(self):
        if os.path.exists(self.pidfile):
            os.remove(self.pidfile)

    def on_signal(self, signum, frame):
        logger.info('receive signal %d closing' % signum)
        if self.subprocess_p:
            self.subprocess_p.terminate()
        self.try_remove_pidfile()
        tornado.ioloop.IOLoop.instance().stop()

    def start(self):
        #signal.signal(signal.SIGINT, self.on_signal)
        #signal.signal(signal.SIGTERM, self.on_signal)
        daemonize(pidfile=self.pidfile)
        #self.start_subprocess()
        start_server()
        self.try_remove_pidfile()

if __name__ == "__main__":
    run()
