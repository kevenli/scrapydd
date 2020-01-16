import json
import logging
from io import BytesIO
from tornado.web import authenticated
from tornado import gen
from .base import AppBaseHandler
from scrapydd.schedule import JobRunning
from scrapydd.models import session_scope, Project, Spider, session_scope, SpiderExecutionQueue, Trigger, SpiderParameter
from scrapydd.storage import ProjectStorage
from scrapydd.workspace import ProcessFailed, InvalidProjectEgg


logger = logging.getLogger(__name__)


class MainHandler(AppBaseHandler):
    @authenticated
    def get(self):
        with session_scope() as session:
            projects = list(session.query(Project).order_by(Project.name))
            self.render('index.html', projects=projects)


class RunSpiderHandler(AppBaseHandler):
    def initialize(self, scheduler_manager):
        super(RunSpiderHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    @authenticated
    def post(self, project_name, spider_name):
        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()
            if not project:
                return self.set_status(404, 'project not found.')

            spider = session.query(Spider).filter_by(name=spider_name, project_id = project.id).first()
            if not spider:
                return self.set_status(404, 'spider not found.')
            try:
                job = self.scheduler_manager.add_task(project_name, spider_name)
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
    def post(self, project_name):
        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()
            project_storage = ProjectStorage(self.settings.get('project_storage_dir'), project)
            for spider in project.spiders:
                triggers = session.query(Trigger).filter_by(spider_id=spider.id)
                session.query(SpiderExecutionQueue).filter_by(spider_id=spider.id).delete()
                session.query(SpiderParameter).filter_by(spider_id=spider.id).delete()
                session.commit()
                for trigger in triggers:
                    self.scheduler_manager.remove_schedule(project_name, spider.name, trigger_id=trigger.id)
                session.query(SpiderExecutionQueue).filter_by(spider_id=spider.id).delete()
                for historical_job in spider.historical_jobs:
                    project_storage.delete_job_data(historical_job)
                    session.delete(historical_job)
                session.delete(spider)
            project_storage.delete_egg()
            session.delete(project.package)
            session.delete(project)


class ProjectSettingsHandler(AppBaseHandler):
    @authenticated
    def get(self, project_name):
        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()

            return self.render('projects/settings.html', project=project)


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
