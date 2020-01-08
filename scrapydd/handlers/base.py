import logging
import json
import os
import tornado.web
import tornado.template
from scrapydd.models import session_scope, User
from scrapydd.security import CookieAuthenticationProvider, HmacAuthorize, BasicAuthentication
from scrapydd.workspace import VenvRunner, DockerRunner


logger = logging.getLogger(__name__)

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')


class AppBaseHandler(tornado.web.RequestHandler):
    authentication_providers = [CookieAuthenticationProvider()]

    def get_default_user(self):
        with session_scope() as session:
            return session.query(User).get(1)

    def get_current_user(self):
        if not self.settings.get('enable_authentication', False):
            return self.get_default_user()

        username = None
        for authentication_provider in self.authentication_providers:
            username = authentication_provider.get_user(self)
            if username:
                break

        if not username:
            return None
        with session_scope() as session:
            return session.query(User).filter_by(username=username).first()

    def data_received(self, chunk):
        pass

    def get_project_workspace(self, project_name):
        project_workspace_cls = self.settings.get('project_workspace_cls')
        return project_workspace_cls(project_name)

    def build_runner(self, eggf):
        runner_type = self.settings.get('runner_type', 'venv')
        if runner_type == 'venv':
            runner = VenvRunner(eggf)
            runner.debug = self.settings.get('debug')
        elif runner_type == 'docker':
            runner = DockerRunner(eggf)
            runner.debug = self.settings.get('debug')
        else:
            raise Exception("Not supported runner_type: %s" % runner_type)
        return runner


class RestBaseHandler(AppBaseHandler):
    authentication_providers = [HmacAuthorize(),  BasicAuthentication()]

    def check_xsrf_cookie(self):
        return None

    def send_json(self, data):
        self.set_header('Content-type', 'application/json')
        if isinstance(data, str):
            self.write(data)
        elif isinstance(data, dict):
            self.write(json.dumps(data))


class InputValidationError(Exception):
    def __init__(self, message=None):
        super(InputValidationError, self).__init__()
        self.message = message
