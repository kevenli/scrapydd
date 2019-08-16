from .base import RestBaseHandler
from ..models import session_scope, Spider, SpiderExecutionQueue, Project, SpiderSettings, NodeKey, Node
import logging
import json
from tornado.web import authenticated
from ..security import generate_digest
import hmac

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
