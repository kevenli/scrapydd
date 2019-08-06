from .base import RestBaseHandler
from ..models import session_scope, Spider, SpiderExecutionQueue, Project, SpiderSettings
import logging
import json
from tornado.web import authenticated

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