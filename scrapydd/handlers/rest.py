from StringIO import StringIO

from tornado import gen

from ..eggstorage import FilesystemEggStorage
from ..exceptions import InvalidProjectEgg, ProcessFailed
from ..workspace import ProjectWorkspace
from .base import RestBaseHandler
from ..models import session_scope, Spider, SpiderExecutionQueue, Project, SpiderSettings, NodeKey, Node
import logging
import json
from tornado.web import authenticated
from ..security import generate_digest
import hmac
from ..schedule import JobRunning

logger = logging.getLogger(__name__)


class GetNextJobHandler(RestBaseHandler):
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
                spider = session.query(Spider).filter_by(id=next_task.spider_id).first()
                if not spider:
                    logger.error('Task %s has not spider, deleting.' % next_task.id)
                    session.query(SpiderExecutionQueue).filter_by(id=next_task.id).delete()
                    self.set_status(400)
                    self.write({'data': None})
                    return

                project = session.query(Project).filter_by(id=spider.project_id).first()
                if not project:
                    logger.error('Task %s has not project, deleting.' % next_task.id)
                    session.query(SpiderExecutionQueue).filter_by(id=next_task.id).delete()
                    self.set_status(400)
                    self.write({'data': None})
                    return

                extra_requirements_setting = session.query(SpiderSettings) \
                    .filter_by(spider_id=spider.id, setting_key='extra_requirements').first()

                extra_requirements = extra_requirements_setting.value if extra_requirements_setting else ''
                response_data['data'] = {'task': {
                    'task_id': next_task.id,
                    'spider_id': next_task.spider_id,
                    'spider_name': next_task.spider_name,
                    'project_name': next_task.project_name,
                    'version': project.version,
                    'extra_requirements': extra_requirements,
                    'spider_parameters': {parameter.parameter_key: parameter.value for parameter in spider.parameters}
                }}
            self.write(json.dumps(response_data))


class RestRegisterNodeHandler(RestBaseHandler):
    @authenticated
    def post(self):
        node_key = self.current_user
        with session_scope() as session:
            tags = self.get_argument('tags', '').strip()
            tags = None if tags == '' else tags
            remote_ip = self.request.remote_ip
            node_manager = self.settings.get('node_manager')
            node = node_manager.create_node(remote_ip, tags=tags, key_id=node_key.id)
            node_key.used_node_id = node.id
            session.add(node_key)
            session.commit()
            self.send_json({'id': node.id})


    def get_current_user(self):
        authorization = self.request.headers.get("Authorization", "").split(" ")
        if len(authorization) != 3:
            logging.info("Invalid Authorization header {}".format(authorization))
            return None

        algorithm, key, provided_digest = authorization
        if algorithm != "HMAC":
            logging.info("Invalid algorithm {}".format(algorithm))
            return None

        with session_scope() as session:
            user_key = session.query(NodeKey).filter_by(key=key).first()

            if user_key is None:
                logging.info("Invalid HMAC key {}".format(key))
                return None

            if user_key.is_deleted:
                logging.info("Invalid HMAC key {}".format(key))
                return None

            secret = user_key.secret_key
            expected_digest = generate_digest(
                secret, self.request.method, self.request.path, self.request.query,
                self.request.body)

            if not hmac.compare_digest(expected_digest, provided_digest):
                logging.info("Invalid HMAC digest {}".format(provided_digest))
                return None

            return user_key


class AddVersionHandler(RestBaseHandler):
    @authenticated
    @gen.coroutine
    def post(self):
        project_name = self.request.arguments['project'][0]
        version = self.request.arguments['version'][0]
        eggfile = self.request.files['egg'][0]
        eggf = StringIO(eggfile['body'])

        try:
            workspace = ProjectWorkspace(project_name)

            yield workspace.init()
            workspace.put_egg(eggf, version)
            yield workspace.install_requirements()
            spiders = yield workspace.spider_list()

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

        logger.debug('spiders: %s' % spiders)
        with session_scope() as session:
            storage = FilesystemEggStorage({})
            eggf.seek(0)
            storage.put(eggf, project_name, version)
            project = session.query(Project).filter_by(name=project_name).first()
            if project is None:
                project = Project()
                project.name = project_name
            project.version = version
            session.add(project)
            session.commit()
            session.refresh(project)

            for spider_name in spiders:
                spider = session.query(Spider).filter_by(project_id=project.id, name=spider_name).first()
                if spider is None:
                    spider = Spider()
                    spider.name = spider_name
                    spider.project_id = project.id
                    session.add(spider)
                    session.commit()
                    session.refresh(spider)

                session.commit()
            self.write(json.dumps({'status': 'ok', 'spiders': len(spiders)}))


class ScheduleHandler(RestBaseHandler):
    def initialize(self, scheduler_manager):
        super(ScheduleHandler, self).initialize()
        self.scheduler_manager = scheduler_manager

    @authenticated
    def post(self):
        project = self.get_argument('project')
        spider = self.get_argument('spider')

        try:
            job = self.scheduler_manager.add_task(project, spider)
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