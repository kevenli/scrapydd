"""
Rest api handlers
"""
# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
import hmac
import logging
import json
from tornado import gen
from tornado.web import authenticated
from six import BytesIO
from six import ensure_str
from ..exceptions import InvalidProjectEgg, ProcessFailed, ProjectNotFound
from ..exceptions import SpiderNotFound, InvalidCronExpression
from .base import RestBaseHandler
from ..models import session_scope, Spider, SpiderExecutionQueue, Project
from ..models import SpiderSettings, NodeKey
from ..models import Trigger, SpiderParameter
from ..security import generate_digest
from ..schedule import JobRunning
from ..storage import ProjectStorage

LOGGER = logging.getLogger(__name__)


class GetNextJobHandler(RestBaseHandler):
    # pylint: disable=arguments-differ
    scheduler_manager = None

    def initialize(self, scheduler_manager=None):
        super(GetNextJobHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    @authenticated
    def post(self):
        with session_scope() as session:
            node_id = int(self.request.arguments['node_id'][0])
            next_task = self.scheduler_manager.get_next_task(node_id)

            response_data = {'data': None}

            if next_task is not None:
                spider = session.query(Spider)\
                    .filter_by(id=next_task.spider_id).first()
                if not spider:
                    LOGGER.error('Task %s has not spider, deleting.',
                                 next_task.id)
                    session.query(SpiderExecutionQueue)\
                        .filter_by(id=next_task.id).delete()
                    self.set_status(400)
                    self.write({'data': None})
                    return

                project = session.query(Project)\
                    .filter_by(id=spider.project_id).first()
                if not project:
                    LOGGER.error('Task %s has not project, deleting.',
                                 next_task.id)
                    session.query(SpiderExecutionQueue)\
                        .filter_by(id=next_task.id).delete()
                    self.set_status(400)
                    self.write({'data': None})
                    return

                extra_requirements_setting = session.query(SpiderSettings) \
                    .filter_by(spider_id=spider.id,
                               setting_key='extra_requirements').first()

                extra_requirements = extra_requirements_setting.value if \
                    extra_requirements_setting else ''
                response_data['data'] = {'task': {
                    'task_id': next_task.id,
                    'spider_id': next_task.spider_id,
                    'spider_name': next_task.spider_name,
                    'project_name': next_task.project_name,
                    'version': project.version,
                    'extra_requirements': extra_requirements,
                    'spider_parameters': {
                        parameter.parameter_key: parameter.value
                        for parameter in spider.parameters}
                }}
            self.write(json.dumps(response_data))


class RestRegisterNodeHandler(RestBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    def post(self):
        node_key = self.current_user
        with session_scope() as session:
            tags = self.get_argument('tags', '').strip()
            tags = None if tags == '' else tags
            remote_ip = self.request.remote_ip
            node_manager = self.settings.get('node_manager')
            node = node_manager.create_node(remote_ip,
                                            tags=tags, key_id=node_key.id)
            node_key.used_node_id = node.id
            session.add(node_key)
            session.commit()
            self.send_json({'id': node.id})


    def get_current_user(self):
        authorization = self.request.headers.get("Authorization", "")\
            .split(" ")
        if len(authorization) != 3:
            logging.info("Invalid Authorization header %s", authorization)
            return None

        algorithm, key, provided_digest = authorization
        if algorithm != "HMAC":
            logging.info("Invalid algorithm %s", algorithm)
            return None

        with session_scope() as session:
            user_key = session.query(NodeKey).filter_by(key=key).first()

            if user_key is None:
                logging.info("Invalid HMAC key %s", key)
                return None

            if user_key.is_deleted:
                logging.info("Invalid HMAC key %s", key)
                return None

            secret = user_key.secret_key
            expected_digest = generate_digest(
                secret, self.request.method, self.request.path,
                self.request.query,
                self.request.body)

            if not hmac.compare_digest(expected_digest, provided_digest):
                logging.info("Invalid HMAC digest %s", provided_digest)
                return None

            return user_key


class AddVersionHandler(RestBaseHandler):
    # pylint: disable=arguments-differ
    @authenticated
    @gen.coroutine
    def post(self):
        project_name = ensure_str(self.get_body_argument('project'))
        version = ensure_str(self.get_body_argument('version'))
        eggfile = self.request.files['egg'][0]
        eggf = BytesIO(eggfile['body'])

        project_manager = self.settings.get('project_manager')
        try:
            project = yield project_manager.upload_project(
                self.current_user, project_name,
                version, eggf)
            with session_scope() as session:
                project = session.query(Project).get(project.id)
                spiders = project.spiders

        except InvalidProjectEgg as ex:
            LOGGER.error('Error when uploading project, %s %s',
                         ex.message,
                         ex.detail)
            self.set_status(400, reason=ex.message)
            self.finish("<html><title>%(code)d: %(message)s</title>"
                        "<body><pre>%(output)s</pre></body></html>" % {
                            "code": 400,
                            "message": ex.message,
                            "output": ex.detail,
                        })
            return
        except ProcessFailed as ex:
            LOGGER.error('Error when uploading project, %s, %s',
                         ex.message,
                         ex.std_output)
            self.set_status(400, reason=ex.message)
            self.finish("<html><title>%(code)d: %(message)s</title>"
                        "<body><pre>%(output)s</pre></body></html>" % {
                            "code": 400,
                            "message": ex.message,
                            "output": ex.std_output,
                        })
            return
        self.write(json.dumps({'status': 'ok', 'spiders': len(spiders)}))


class ScheduleHandler(RestBaseHandler):
    # pylint: disable=arguments-differ
    def initialize(self, scheduler_manager):
        super(ScheduleHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    @authenticated
    def post(self):
        project = self.get_body_argument('project')
        spider = self.get_body_argument('spider')

        try:
            job = self.scheduler_manager.add_task(project, spider)
            jobid = job.id
            response_data = {
                'status': 'ok',
                'jobid': jobid
            }
            self.write(json.dumps(response_data))
        except JobRunning as ex:
            response_data = {
                'status': 'error',
                'errormsg': 'job is running with jobid %s' % ex.jobid
            }
            self.set_status(400, 'job is running')
            self.write(json.dumps(response_data))


class DeleteProjectHandler(RestBaseHandler):
    # pylint: disable=arguments-differ
    def initialize(self, scheduler_manager):
        super(DeleteProjectHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    @authenticated
    def post(self):
        project_name = self.get_argument('project')
        with session_scope() as session:
            project = session.query(Project)\
                .filter_by(name=project_name).first()
            project_storage = ProjectStorage(
                self.settings.get('project_storage_dir'), project)
            if not project:
                return self.set_status(404, 'project not found.')
            spiders = session.query(Spider).filter_by(project_id=project.id)
            for spider in spiders:
                triggers = session.query(Trigger)\
                    .filter_by(spider_id=spider.id)
                session.query(SpiderExecutionQueue)\
                    .filter_by(spider_id=spider.id).delete()
                session.query(SpiderParameter)\
                    .filter_by(spider_id=spider.id).delete()
                session.commit()
                for trigger in triggers:
                    self.scheduler_manager\
                        .remove_schedule(project_name,
                                         spider.name,
                                         trigger_id=trigger.id)

                for job in spider.historical_jobs:
                    project_storage.delete_job_data(job)
                    session.delete(job)
                session.delete(spider)

            project_storage.delete_egg()
            session.delete(project.package)
            session.delete(project)

        LOGGER.info('project %s deleted', project_name)
        return self.write('Project deleted.')


class AddScheduleHandler(RestBaseHandler):
    # pylint: disable=arguments-differ
    def initialize(self, scheduler_manager):
        super(AddScheduleHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    @authenticated
    def post(self):
        project_name = self.get_argument('project')
        spider_name = self.get_argument('spider')
        cron = self.get_argument('cron')

        with session_scope() as session:
            try:
                spider = self.get_spider(session, project_name, spider_name)
            except SpiderNotFound:
                response_data = {
                    'status': 'error',
                    'errormsg': 'spider not found',
                }
                self.set_status(404)
                return self.write(json.dumps(response_data))
            except ProjectNotFound:
                response_data = {
                    'status': 'error',
                    'errormsg': 'project not found',
                }
                self.set_status(404)
                return self.write(json.dumps(response_data))
            try:
                self.scheduler_manager.add_schedule(spider.project,
                                                    spider, cron)
                response_data = {
                    'status': 'ok',
                }
                return self.write(json.dumps(response_data))

            except InvalidCronExpression:
                response_data = {
                    'status': 'error',
                    'errormsg': 'invalid cron expression.',
                }
                self.set_status(400)
                return self.write(json.dumps(response_data))
