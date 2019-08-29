from .base import AppBaseHandler
from tornado.web import authenticated
from ..schedule import JobRunning
import json
from ..models import Project, Spider, session_scope

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
            if not project:
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