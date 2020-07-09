import json
import logging
from io import BytesIO
from tornado.web import authenticated
from tornado import gen
from .base import AppBaseHandler
from scrapydd.schedule import JobRunning
from scrapydd.models import Project, Spider, session_scope, SpiderExecutionQueue, Trigger, SpiderParameter
from scrapydd.models import HistoricalJob
from scrapydd.storage import ProjectStorage
from scrapydd.workspace import ProcessFailed, InvalidProjectEgg


logger = logging.getLogger(__name__)


class MainHandler(AppBaseHandler):
    @authenticated
    def get(self):
        with session_scope() as session:
            #projects = list(session.query(Project).order_by(Project.name))
            projects = self.project_manager.get_projects(session, self.current_user)
            self.render('index.html', projects=projects)


class RunSpiderHandler(AppBaseHandler):
    def initialize(self, scheduler_manager):
        super(RunSpiderHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    @authenticated
    def post(self, project_id, spider_id):
        session = self.session
        spider = self.project_manager.get_spider(session, self.current_user,
                                                    project_id, spider_id)
        try:
            job = self.scheduler_manager.add_spider_task(session, spider)
            jobid = job.id
            response_data = {
                'status': 'ok',
                'jobid': jobid
            }
            self.write(json.dumps(response_data))
        except JobRunning as e:
            response_data = {
                'status': 'error',
                'errormsg': 'job is running with jobid %s' % e.jobid
            }
            self.set_status(400, 'job is running')
            self.write(json.dumps(response_data))


class DeleteProjectHandler(AppBaseHandler):
    @authenticated
    def post(self, project_id):
        with session_scope() as session:
            project = self.project_manager.get_project(session, self.current_user, project_id)

        project_manager = self.settings.get('project_manager')
        project_manager.delete_project(self.current_user, project_id=project.id)


class ProjectSettingsHandler(AppBaseHandler):
    @authenticated
    def get(self, project_id):
        with session_scope() as session:
            project = self.project_manager.get_project(session, self.current_user,
                                                       project_id)

            return self.render('projects/settings.html', project=project)


class ProjectPackageHandler(AppBaseHandler):
    @authenticated
    def get(self, project_id):
        with session_scope() as session:
            project = self.project_manager.get_project(session, self.current_user,
                                                       project_id)

            return self.render('projects/package.html', project=project)

    @authenticated
    @gen.coroutine
    def post(self, project_id):
        with session_scope() as session:
            project = self.project_manager.get_project(session, self.current_user,
                                                       project_id)
            version = self.get_body_argument('version')
            eggfile = self.request.files['egg'][0]
            eggf = BytesIO(eggfile['body'])
            project_manager = self.settings.get('project_manager')
            project = yield project_manager.upload_project_package(session, project, eggf, version, True)
            return self.render('projects/package.html', project=project)


class UploadProject(AppBaseHandler):
    except_project_settings = ['NEWSPIDER_MODULE',
                               'SPIDER_MODULES',
                               'BOT_NAME',
                               'USER_AGENT',
                               'LOG_LEVEL',
                               'PROJECT',
                               'SETTINGS_MODULE',
                               ]

    @authenticated
    @gen.coroutine
    def post(self):
        project_name = self.get_body_argument('project')
        version = self.get_body_argument('version')
        eggfile = self.request.files['egg'][0]
        eggf = BytesIO(eggfile['body'])
        project_manager = self.settings.get('project_manager')

        try:
            project = yield project_manager.upload_project(self.current_user, project_name,
                                                           version, eggf)
            with session_scope() as session:
                project = session.query(Project).get(project.id)
                spiders = project.spiders

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

        if self.request.path.endswith('.json'):
            self.write(json.dumps({'status': 'ok', 'spiders': len(spiders)}))
        else:
            self.render("uploadproject.html")

    @authenticated
    def get(self):
        self.render("uploadproject.html")


class GetAllPluginsHandler(AppBaseHandler):
    @authenticated
    def get(self):
        spider_plugin_manager = self.settings.get('spider_plugin_manager')
        plugins = spider_plugin_manager.get_all_plugins()


class ItemsFileHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def get(self, project_id, spider_id, job_id):
        with session_scope() as session:
            spider = self.project_manager.get_spider(session, self.current_user,
                                                     project_id, spider_id)
            job = session.query(HistoricalJob).filter_by(
                spider_id=spider.id,
                id=job_id).first()
            if not job:
                return self.set_status(404, 'object not found.')
            project_storage = ProjectStorage(
                self.settings.get('project_storage_dir'), job.spider.project)
            self.set_header('Content-Type', 'application/json')
            return self.write(project_storage.get_job_items(job).read())


class LogsHandler(AppBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def get(self, project_id, spider_id, job_id):
        with session_scope() as session:
            spider = self.project_manager.get_spider(session, self.current_user,
                                                     project_id, spider_id)
            job = session.query(HistoricalJob).filter_by(
                spider_id=spider.id,
                id=job_id).first()
            project_storage = ProjectStorage(
                self.settings.get('project_storage_dir'), job.spider.project)
            log = project_storage.get_job_log(job).read()
            self.set_header('Content-Type', 'text/plain')
            self.write(log)


class NewProject(AppBaseHandler):
    @authenticated
    def get(self):
        self.render('projects/new.html')

    @authenticated
    def post(self):
        project_name = self.get_body_argument('project_name')
        project_manager = self.settings.get('project_manager')
        new_project = project_manager.create_project(self.session, self.current_user, project_name)
        return self.redirect(f'/projects/{new_project.id}/package')


class ProjectInfoHandler(AppBaseHandler):
    @authenticated
    def get(self, project_id):
        with session_scope() as session:
            project_manager = self.settings.get('project_manager')
            project = project_manager.get_project(session, self.current_user,
                                                  project_id)
            return self.render('projects/info.html', project=project)


def spider_url(handler, spider, *args):
    return '/projects/%s/spiders/%s' % (spider.project.id, spider.id)
