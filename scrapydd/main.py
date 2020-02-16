# -*-coding:utf8-*-
"""
Main entrypoint of scrapydd server
"""
# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
import ssl
import os.path
import sys
import logging
import subprocess
import signal
import json
# pylint: disable=deprecated-module
from optparse import OptionParser
import tornado.ioloop
import tornado.web
import tornado.template
import tornado.httpserver
import tornado.netutil
from tornado.web import authenticated
from sqlalchemy import desc
from .models import Session, Project, Spider, SpiderExecutionQueue
from .models import init_database, HistoricalJob
from .models import session_scope, SpiderSettings, WebhookJob
from .models import SpiderParameter
from .schedule import SchedulerManager
from .nodes import NodeManager
from .config import Config
from .process import fork_processes
from .project import ProjectManager
from .exceptions import SpiderNotFound, ProjectNotFound
from .exceptions import InvalidCronExpression
from .webhook import WebhookDaemon
from .daemonize import daemonize
from .workspace import RunnerFactory
from .cluster import ClusterNode
from .spiderplugin import SpiderPluginManager
from .ssl_gen import SSLCertificateGenerator
from .settting import SpiderSettingLoader
from .security import NoAuthenticationProvider
from .handlers.auth import LogoutHandler, SigninHandler
from .handlers.base import AppBaseHandler, RestBaseHandler
from .handlers import admin, profile, rest
from .handlers.node import NodesHandler, ExecuteNextHandler
from .handlers.node import ExecuteCompleteHandler, NodeHeartbeatHandler
from .handlers.node import JobStartHandler, RegisterNodeHandler, JobEggHandler
from .handlers import webui
from .storage import ProjectStorage
from .scripts.upgrade_filestorage import upgrade as upgrade_project_storage
from .scripts.upgrade_projectpackage import upgrade as upgrade_project_package

LOGGER = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(__file__)


class ProjectList(AppBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def get(self):
        session = Session()
        projects = session.query(Project)

        response_data = {'projects': {'id': item.id for item in projects}}
        self.write(response_data)
        session.close()


class SpiderInstanceHandler2(AppBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def get(self, project, spider):
        session = Session()
        project = session.query(Project) \
            .filter(Project.name == project).first()
        spider = session.query(Spider) \
            .filter(Spider.project_id == project.id,
                    Spider.name == spider).first()
        jobs = session.query(HistoricalJob) \
            .filter(HistoricalJob.spider_id == spider.id) \
            .order_by(desc(HistoricalJob.start_time)) \
            .slice(0, 100)
        running_jobs = session.query(SpiderExecutionQueue) \
            .filter(SpiderExecutionQueue.spider_id == spider.id) \
            .order_by(desc(SpiderExecutionQueue.update_time))

        jobs_count = session.query(HistoricalJob) \
            .filter(HistoricalJob.spider_id == spider.id).count()

        webhook_jobs = session.query(WebhookJob).filter_by(spider_id=spider.id)

        context = {}
        context['spider'] = spider
        context['project'] = project
        context['jobs'] = jobs
        context['running_jobs'] = running_jobs
        context['settings'] = session.query(SpiderSettings)\
            .filter_by(spider_id=spider.id)\
            .order_by(SpiderSettings.setting_key)
        context['webhook_jobs'] = webhook_jobs
        spider_parameters = session.query(SpiderParameter) \
            .filter_by(spider_id=spider.id) \
            .order_by(SpiderParameter.parameter_key)
        context['spider_parameters'] = {
            parameter.parameter_key: parameter.value for parameter in
            spider_parameters}
        self.render("spider.html", jobs_count=jobs_count, **context)
        session.close()


class SpiderEggHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def get(self, spider_id):
        session = Session()
        spider = session.query(Spider).filter_by(id=spider_id).first()
        project_storage = ProjectStorage(
            self.settings.get('project_storage_dir'),
            project=spider.project)
        _, f_egg = project_storage.get_egg()
        self.write(f_egg.read())
        session.close()


class ProjectSpiderEggHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def get(self, project, spider):
        with session_scope() as session:
            project = session.query(Project) \
                .filter(Project.name == project).first()
            spider = session.query(Spider) \
                .filter(Spider.project_id == project.id,
                        Spider.name == spider).first()
            project_storage = ProjectStorage(
                self.settings.get('project_storage_dir'),
                project)
            _, f_egg = project_storage.get_egg()
            self.write(f_egg.read())
            session.close()


class SpiderListHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def get(self):
        session = Session()
        spiders = session.query(Spider)
        self.render("spiderlist.html", spiders=spiders)
        session.close()


class SpiderTriggersHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    scheduler_manager = None

    def initialize(self, scheduler_manager=None):
        assert isinstance(scheduler_manager, SchedulerManager)
        assert scheduler_manager
        super(SpiderTriggersHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    @authenticated
    def get(self, project, spider):
        with session_scope() as session:
            project = session.query(Project) \
                .filter(Project.name == project).first()
            spider = session.query(Spider) \
                .filter(Spider.project_id == project.id,
                        Spider.name == spider).first()
            context = {'spider': spider, 'errormsg': None}
            self.render("spidercreatetrigger.html", **context)

    @authenticated
    def post(self, project, spider):
        cron = self.get_argument('cron')
        with session_scope() as session:
            project = session.query(Project) \
                .filter(Project.name == project).first()

            spider = session.query(Spider) \
                .filter(Spider.project_id == project.id,
                        Spider.name == spider).first()
            try:
                self.scheduler_manager.add_schedule(project, spider, cron)
                return self.redirect('/projects/%s/spiders/%s' % (
                    project.name, spider.name))
            except InvalidCronExpression:
                context = {'spider': spider,
                           'errormsg': 'Invalid cron expression.'}
                return self.render("spidercreatetrigger.html", **context)


class DeleteSpiderTriggersHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    scheduler_manager = None

    def initialize(self, scheduler_manager=None):
        super(DeleteSpiderTriggersHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    @authenticated
    def post(self, project_name, spider_name, trigger_id):
        self.scheduler_manager \
            .remove_schedule(project_name, spider_name, trigger_id)
        self.redirect('/projects/%s/spiders/%s' % (project_name, spider_name))


class DeleteSpiderJobHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def post(self, project_name, spider_name, job_id):
        with session_scope() as session:
            try:
                spider = self.get_spider(session, project_name, spider_name)
            except ProjectNotFound:
                return self.set_status(404, 'project not found.')
            except SpiderNotFound:
                return self.set_status(404, 'spider not found.')

            job = session.query(HistoricalJob) \
                .filter_by(spider_id=spider.id, id=job_id).first()
            if not job:
                return self.set_status(404, 'job not found.')

            project_storage = ProjectStorage(
                self.settings.get('project_storage_dir'), spider.project)
            project_storage.delete_job_data(job)
            session.delete(job)
            session.commit()
            return self.write('Success.')


class JobsHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    def initialize(self, scheduler_manager):
        super(JobsHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    @authenticated
    def get(self):
        pending, running, finished = self.scheduler_manager.jobs()
        context = {
            'pending': pending,
            'running': running,
            'finished': finished,
        }
        self.render("jobs.html", **context)


class LogsHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def get(self, project_name, spider_name, job_id):
        with session_scope() as session:
            try:
                spider = self.get_spider(session, project_name, spider_name)
            except ProjectNotFound:
                return self.set_status(404, 'project not found.')
            except SpiderNotFound:
                return self.set_status(404, 'spider not found.')
            job = session.query(HistoricalJob).filter_by(
                spider_id=spider.id,
                id=job_id).first()
            project_storage = ProjectStorage(
                self.settings.get('project_storage_dir'), job.spider.project)
            log = project_storage.get_job_log(job).read()
            self.set_header('Content-Type', 'text')
            self.write(log)


class ItemsFileHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def get(self, project_name, spider_name, job_id):
        with session_scope() as session:
            try:
                spider = self.get_spider(session, project_name, spider_name)
            except ProjectNotFound:
                return self.set_status(404, 'project not found.')
            except SpiderNotFound:
                return self.set_status(404, 'spider not found.')
            job = session.query(HistoricalJob).filter_by(
                spider_id=spider.id,
                id=job_id).first()
            project_storage = ProjectStorage(
                self.settings.get('project_storage_dir'), job.spider.project)
            self.set_header('Content-Type', 'application/json')
            return self.write(project_storage.get_job_items(job).read())


class SpiderWebhookHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def get(self, project_name, spider_name):
        session = Session()
        project = session.query(Project) \
            .filter(Project.name == project_name).first()
        spider = session.query(Spider) \
            .filter(Spider.project_id == project.id,
                    Spider.name == spider_name).first()
        webhook_setting = session.query(SpiderSettings) \
            .filter_by(spider_id=spider.id,
                       setting_key='webhook_payload').first()
        if webhook_setting:
            self.write(webhook_setting.value)

    @authenticated
    def post(self, project_name, spider_name):
        payload_url = self.get_argument('payload_url')
        with session_scope() as session:
            project = session.query(Project) \
                .filter(Project.name == project_name).first()
            spider = session.query(Spider) \
                .filter(Spider.project_id == project.id,
                        Spider.name == spider_name).first()
            webhook_setting = session.query(SpiderSettings) \
                .filter_by(spider_id=spider.id,
                           setting_key='webhook_payload').first()
            if webhook_setting is None:
                # no existing row
                webhook_setting = SpiderSettings()
                webhook_setting.spider_id = spider.id
                webhook_setting.setting_key = 'webhook_payload'

            webhook_setting.value = payload_url
            session.add(webhook_setting)
            session.commit()

    @authenticated
    def put(self, project_name, spider_name):
        self.post(project_name, spider_name)

    @authenticated
    def delete(self, project_name, spider_name):
        with session_scope() as session:
            project = session.query(Project) \
                .filter(Project.name == project_name).first()
            spider = session.query(Spider) \
                .filter(Spider.project_id == project.id,
                        Spider.name == spider_name).first()
            session.query(SpiderSettings) \
                .filter_by(spider_id=spider.id,
                           setting_key='webhook_payload').delete()
            session.commit()


class SpiderSettingsHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    available_settings = {
        'concurrency': r'\d+',
        'timeout': r'\d+',
        'webhook_payload': '.*',
        'webhook_batch_size': r'\d+',
        'tag': '.*',
        'extra_requirements': '.*'
    }

    @authenticated
    def get(self, project, spider):
        with session_scope() as session:
            project = session.query(Project) \
                .filter_by(name=project).first()
            spider = session.query(Spider) \
                .filter_by(project_id=project.id, name=spider).first()
            job_settings = {
                setting.setting_key: setting.value for setting in
                session.query(SpiderSettings).filter_by(spider_id=spider.id)}

            # default setting values
            if 'concurrency' not in job_settings:
                job_settings['concurrency'] = 1
            if 'timeout' not in job_settings:
                job_settings['timeout'] = 3600
            if 'tag' not in job_settings or job_settings['tag'] is None:
                job_settings['tag'] = ''
            context = {}
            context['settings'] = job_settings
            context['project'] = project
            context['spider'] = spider

            spider_parameters = session.query(SpiderParameter) \
                .filter_by(spider_id=spider.id) \
                .order_by(SpiderParameter.parameter_key)
            context['spider_parameters'] = spider_parameters

            return self.render('spidersettings.html', **context)

    @authenticated
    def post(self, project, spider):
        with session_scope() as session:
            project = session.query(Project) \
                .filter_by(name=project).first()
            spider = session.query(Spider) \
                .filter_by(project_id=project.id, name=spider).first()

            setting_concurrency_value = self.get_body_argument('concurrency',
                                                               '1')
            setting_concurrency = session.query(SpiderSettings).filter_by(
                spider_id=spider.id,
                setting_key='concurrency').first()
            if not setting_concurrency:
                setting_concurrency = SpiderSettings()
                setting_concurrency.spider_id = spider.id
                setting_concurrency.setting_key = 'concurrency'
            setting_concurrency.value = setting_concurrency_value
            session.add(setting_concurrency)

            setting_timeout_value = self.get_body_argument('timeout', '3600')
            setting_timeout = session.query(SpiderSettings).filter_by(
                spider_id=spider.id,
                setting_key='timeout').first()
            if not setting_timeout:
                setting_timeout = SpiderSettings()
                setting_timeout.spider_id = spider.id
                setting_timeout.setting_key = 'timeout'
            setting_timeout.value = setting_timeout_value
            session.add(setting_timeout)

            setting_webhook_payload_value = self.get_body_argument(
                'webhook_payload', '')
            setting_webhook_payload = session.query(SpiderSettings) \
                .filter_by(spider_id=spider.id,
                           setting_key='webhook_payload').first()
            if not setting_webhook_payload:
                setting_webhook_payload = SpiderSettings()
                setting_webhook_payload.spider_id = spider.id
                setting_webhook_payload.setting_key = 'webhook_payload'
            setting_webhook_payload.value = setting_webhook_payload_value
            session.add(setting_webhook_payload)

            setting_webhook_batch_size_value = self.get_body_argument(
                'webhook_batch_size', '')
            setting_webhook_batch_size = session.query(SpiderSettings) \
                .filter_by(spider_id=spider.id,
                           setting_key='webhook_batch_size').first()
            if not setting_webhook_batch_size:
                setting_webhook_batch_size = SpiderSettings()
                setting_webhook_batch_size.spider_id = spider.id
                setting_webhook_batch_size.setting_key = 'webhook_batch_size'
            setting_webhook_batch_size.value = setting_webhook_batch_size_value
            session.add(setting_webhook_batch_size)

            setting_tag_value = self.get_body_argument('tag', '').strip()
            setting_tag_value = None if setting_tag_value == '' else \
                setting_tag_value
            setting_tag = session.query(SpiderSettings).filter_by(
                spider_id=spider.id,
                setting_key='tag').first()
            if not setting_tag:
                setting_tag = SpiderSettings()
                setting_tag.spider_id = spider.id
                setting_tag.setting_key = 'tag'
            setting_tag.value = setting_tag_value
            session.add(setting_tag)

            setting_extra_requirements_value = self.get_body_argument(
                'extra_requirements', '').strip()
            setting_extra_requirements_value = None if \
                setting_extra_requirements_value == '' else \
                setting_extra_requirements_value
            setting_extra_requirements = session.query(SpiderSettings) \
                .filter_by(spider_id=spider.id,
                           setting_key='extra_requirements').first()
            if not setting_extra_requirements:
                setting_extra_requirements = SpiderSettings()
                setting_extra_requirements.spider_id = spider.id
                setting_extra_requirements.setting_key = 'extra_requirements'
            setting_extra_requirements.value = setting_extra_requirements_value
            session.add(setting_extra_requirements)

            spider_parameter_keys = self.get_body_arguments(
                'SpiderParameterKey')
            spider_parameter_values = self.get_body_arguments(
                'SpiderParameterValue')
            session.query(SpiderParameter) \
                .filter_by(spider_id=spider.id) \
                .delete()
            for i, spider_parameter_key in enumerate(spider_parameter_keys):
                spider_parameter_key = spider_parameter_key.strip()
                if spider_parameter_key == '':
                    continue

                spider_parameter_value = spider_parameter_values[i]
                spider_parameter = session.query(SpiderParameter).filter_by(
                    spider_id=spider.id,
                    parameter_key=spider_parameter_key).first()
                if not spider_parameter:
                    spider_parameter = SpiderParameter()
                    spider_parameter.spider_id = spider.id
                    spider_parameter.parameter_key = spider_parameter_key
                spider_parameter.value = spider_parameter_value
                session.add(spider_parameter)

            self.redirect('/projects/%s/spiders/%s' % (project.name,
                                                       spider.name))


class CACertHandler(RestBaseHandler):
    # pylint: disable=arguments-differ
    def get(self):
        self.write(open('keys/ca.crt', 'rb').read())
        self.set_header('Content-Type', 'application/cert')


class ListProjectVersionsHandler(RestBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def get(self):
        try:
            project = self.get_argument('project')
        except tornado.web.MissingArgumentError as ex:
            return self.write({'status': 'error', 'message': ex.arg_name})

        project_storage = ProjectStorage(
            self.settings.get('project_storage_dir'), project)
        versions = project_storage.list_egg_versions()
        return self.write({'status': 'ok', 'versions': versions})


def make_app(scheduler_manager, node_manager, webhook_daemon=None,
             authentication_providers=None,
             debug=False,
             enable_authentication=False,
             secret_key='',
             enable_node_registration=False,
             project_storage_dir='.',
             default_project_storage_version=2,
             runner_factory=None,
             project_manager=None,
             spider_plugin_manager=None):
    """

    @type scheduler_manager SchedulerManager
    @type node_manager NodeManager
    @type webhook_daemon: WebhookDaemon
    @type authentication_provider: AuthenticationProvider

    :return: tornado.web.Application
    """
    if project_manager is None:
        project_manager = ProjectManager(runner_factory, project_storage_dir,
                                         scheduler_manager,
                                         default_project_storage_version)

    settings = dict(cookie_secret=secret_key,
                    login_url="/signin",
                    static_path=os.path.join(BASE_DIR, 'static'),
                    template_path=os.path.join(BASE_DIR, 'templates'),
                    xsrf_cookies=True,
                    debug=debug,
                    enable_authentication=enable_authentication,
                    scheduler_manager=scheduler_manager,
                    enable_node_registration=enable_node_registration,
                    project_storage_dir=project_storage_dir,
                    default_project_storage_version=
                    default_project_storage_version,
                    runner_factory=runner_factory,
                    project_manager=project_manager,
                    spider_plugin_manager=spider_plugin_manager,
                    )

    if authentication_providers is None:
        authentication_providers = []

    if len(authentication_providers) == 0:
        authentication_providers.append(NoAuthenticationProvider())

    settings['authentication_providers'] = authentication_providers
    settings['node_manager'] = node_manager

    return tornado.web.Application([
        (r"/", webui.MainHandler),
        (r'/signin', SigninHandler),
        (r'/logout', LogoutHandler),
        (r'/uploadproject', webui.UploadProject),

        # scrapyd apis
        (r'/addversion.json', rest.AddVersionHandler),
        (r'/delproject.json', rest.DeleteProjectHandler,
         {'scheduler_manager': scheduler_manager}),
        (r'/listversions.json', ListProjectVersionsHandler),
        (r'/schedule.json', rest.ScheduleHandler,
         {'scheduler_manager': scheduler_manager}),

        (r'/add_schedule.json', rest.AddScheduleHandler,
         {'scheduler_manager': scheduler_manager}),
        (r'/projects', ProjectList),
        (r'/spiders', SpiderListHandler),
        (r'/spiders/(\d+)/egg', SpiderEggHandler),
        (r'/projects/(\w+)/spiders/(\w+)', SpiderInstanceHandler2),
        (r'/projects/(\w+)/spiders/(\w+)/triggers', SpiderTriggersHandler,
         {'scheduler_manager': scheduler_manager}),
        (r'/projects/(\w+)/spiders/(\w+)/triggers/(\w+)/delete',
         DeleteSpiderTriggersHandler,
         {'scheduler_manager': scheduler_manager}),
        (r'/projects/(\w+)/spiders/(\w+)/jobs/(\w+)/delete',
         DeleteSpiderJobHandler),
        (r'/projects/(\w+)/spiders/(\w+)/settings', SpiderSettingsHandler),
        (r'/projects/(\w+)/spiders/(\w+)/webhook', SpiderWebhookHandler),
        (r'^/projects/(\w+)/spiders/(\w+)/run$', webui.RunSpiderHandler,
         {'scheduler_manager': scheduler_manager}),
        (r'/projects/(\w+)/spiders/(\w+)/egg', ProjectSpiderEggHandler),
        (r'/projects/(\w+)/delete$', webui.DeleteProjectHandler),
        (r'/projects/(\w+)/settings$', webui.ProjectSettingsHandler),

        (r'/profile$', profile.ProfileHomeHandler),
        (r'/profile/keys$', profile.ProfileKeysHandler),
        (r'/profile/change_password$', profile.ProfileChangepasswordHandler),

        (r'/admin$', admin.AdminHomeHandler),
        (r'^/admin/nodes$', admin.AdminNodesHandler),
        (r'^/admin/spiderplugins$', admin.AdminPluginsHandler),

        # rest apis
        (r'^/api/projects/(\w+)/spiders/(\w+)/jobs/(\w+)', rest.GetProjectJob),
        (r'^/api/projects/(\w+)/spiders/(\w+)/jobs/(\w+)/items',
            rest.GetProjectJobItems),

        # agent node ysing handlers
        (r'/executing/next_task', ExecuteNextHandler,
         {'scheduler_manager': scheduler_manager}),
        (r'/executing/complete', ExecuteCompleteHandler,
         {'webhook_daemon': webhook_daemon,
          'scheduler_manager': scheduler_manager}),
        (r'/nodes', NodesHandler, {'node_manager': node_manager}),
        (r'/nodes/register', RegisterNodeHandler),

        (r'/nodes/(\d+)/heartbeat', NodeHeartbeatHandler,
         {'node_manager': node_manager,
          'scheduler_manager': scheduler_manager}),
        (r'/jobs', JobsHandler, {'scheduler_manager': scheduler_manager}),
        (r'/jobs/(\w+)/start', JobStartHandler,
         {'scheduler_manager': scheduler_manager}),
        (r'/jobs/(\w+)/egg', JobEggHandler),
        (r'/logs/(\w+)/(\w+)/(\w+).log', LogsHandler),
        (r'/items/(\w+)/(\w+)/(\w+).jl', ItemsFileHandler),
        (r'/ca.crt', CACertHandler),
        (r'/static/(.*)', tornado.web.StaticFileHandler,
         {'path': os.path.join(os.path.dirname(__file__), 'static')}),
    ], **settings)


def check_and_gen_ssl_keys(config):
    server_name = config.get('server_name')
    if not server_name:
        raise Exception('Must specify a server name')
    ssl_gen = SSLCertificateGenerator()
    try:
        ssl_gen.get_ca_key()
        ssl_gen.get_ca_cert()
    except IOError:
        LOGGER.info('ca cert not exist, creating new cert and key.')
        ssl_gen.gen_ca('scrapydd', 'scrapydd')

    host_name = config.get('server_name')
    if not os.path.exists(os.path.join('keys', '%s.crt' % host_name)):
        LOGGER.info('server cert not exist, creating new.')
        alt_names = [s.strip() for s in config.get('dns_alt_names').split(',')]
        if '' in alt_names:
            alt_names.remove('')
        ssl_gen.gen_cert(host_name, usage=2, alt_names=alt_names)


def start_server(argv=None):
    config = Config()
    logging.debug('starting server with argv : %s', argv)

    is_debug = config.getboolean('debug')

    init_database()
    upgrade_project_storage()
    upgrade_project_package()
    bind_address = config.get('bind_address')
    bind_port = config.getint('bind_port')
    try:
        https_port = int(config.get('https_port')) if config.get(
            'https_port') else None
    except ValueError:
        LOGGER.warning(
            'https_port is configured, but it is not int, %s',
            config.get('https_port'))
        https_port = None

    if https_port:
        https_sockets = tornado.netutil.bind_sockets(https_port, bind_address)

    sockets = tornado.netutil.bind_sockets(bind_port, bind_address)

    # task_id is current process identifier when forked processes, start with 0
    task_id = None
    if not is_debug:
        if not sys.platform.startswith('win'):
            task_id = fork_processes(config.getint('fork_proc_count'))
        else:
            LOGGER.warning((
                'Windows platform does not support forking process,'
                'running in single process mode.'))

    cluster_sync_obj = None
    if task_id is not None and config.get('cluster_bind_address'):
        cluster_node = ClusterNode(task_id, config)
        cluster_sync_obj = cluster_node.sync_obj

    scheduler_manager = SchedulerManager(config=config,
                                         syncobj=cluster_sync_obj)
    scheduler_manager.init()

    node_manager = NodeManager(scheduler_manager)
    node_manager.init()

    webhook_daemon = WebhookDaemon(config, SpiderSettingLoader())
    webhook_daemon.init()

    runner_factory = RunnerFactory(config)

    spider_plugin_manager = SpiderPluginManager()

    enable_authentication = config.getboolean('enable_authentication')
    secret_key = config.get('secret_key')
    app = make_app(scheduler_manager, node_manager, webhook_daemon,
                   debug=is_debug,
                   enable_authentication=enable_authentication,
                   secret_key=secret_key,
                   enable_node_registration=config.getboolean(
                       'enable_node_registration', False),
                   project_storage_dir=config.get('project_storage_dir'),
                   default_project_storage_version=config.getint(
                       'default_project_storage_version'),
                   runner_factory=runner_factory,
                   spider_plugin_manager=spider_plugin_manager)

    server = tornado.httpserver.HTTPServer(app)
    server.add_sockets(sockets)

    if https_port:
        check_and_gen_ssl_keys(config)
        if config.getboolean('client_validation'):
            ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        else:
            ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.load_cert_chain(
            os.path.join('keys', "%s.crt" % config.get('server_name')),
            os.path.join('keys', "%s.key" % config.get('server_name')))
        ssl_ctx.load_verify_locations(cafile='keys/ca.crt')
        ssl_ctx.check_hostname = False
        httpsserver = tornado.httpserver.HTTPServer(app, ssl_options=ssl_ctx)
        httpsserver.add_sockets(https_sockets)
        LOGGER.info('starting https server on %s:%s', bind_address, https_port)
    ioloop = tornado.ioloop.IOLoop.current()
    ioloop.start()


def run(argv=None):
    if argv is None:
        argv = sys.argv
    parser = OptionParser(prog='scrapydd server')
    parser.add_option('--daemon', action='store_true',
                      help='run scrapydd server in daemon mode')
    parser.add_option('--pidfile',
                      help='pid file will be created when daemon started')
    opts, _ = parser.parse_args(argv)
    pidfile = opts.pidfile or 'scrapydd-server.pid'

    config = Config()
    init_logging(config)

    if opts.daemon:
        print('starting daemon.')
        daemon = Daemon(pidfile=pidfile)
        daemon.start()
        sys.exit(0)
    else:
        start_server()


def init_logging(config):
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logger = logging.getLogger()
    file_handler = logging.handlers.TimedRotatingFileHandler(
        os.path.join(log_dir, 'scrapydd-server.log'), when='D',
        backupCount=7)
    error_handler = logging.handlers.TimedRotatingFileHandler(
        os.path.join(log_dir, 'scrapydd-error.log'), when='D',
        backupCount=30)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    error_handler.setFormatter(formatter)
    error_handler.setLevel(logging.ERROR)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.addHandler(error_handler)

    if config.getboolean('debug'):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    access_log_logger = logging.getLogger('tornado.access')
    access_log_fh = logging.handlers.TimedRotatingFileHandler(
        os.path.join(log_dir, 'scrapydd-access.log'), when='D',
        backupCount=30)
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
            with open(self.pidfile, 'r') as f_pid:
                return int(f_pid.readline())
        except IOError:
            return None

    def try_remove_pidfile(self):
        if os.path.exists(self.pidfile):
            os.remove(self.pidfile)

    def on_signal(self, signum, frame):
        LOGGER.info('receive signal %d closing, frame: %s', signum, frame)
        if self.subprocess_p:
            self.subprocess_p.terminate()
        self.try_remove_pidfile()
        tornado.ioloop.IOLoop.instance().stop()

    def start(self):
        # signal.signal(signal.SIGINT, self.on_signal)
        # signal.signal(signal.SIGTERM, self.on_signal)
        daemonize(pidfile=self.pidfile)
        # self.start_subprocess()
        start_server()
        self.try_remove_pidfile()


if __name__ == "__main__":
    run()
