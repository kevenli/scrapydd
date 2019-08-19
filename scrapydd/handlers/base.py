import tornado.web
import tornado.template
import os
from ..models import session_scope, User, UserKey
import logging
import json

logger = logging.getLogger(__name__)

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')


class AppBaseHandler(tornado.web.RequestHandler):
    authentication_providers = None

    def initialize(self):
        self.authentication_providers = self.settings.get('authentication_providers')

    def get_current_user(self):
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


class RestBaseHandler(AppBaseHandler):
    def check_xsrf_cookie(self):
        return None

    def send_json(self, data):
        self.set_header('Content-type','application/json')
        if isinstance(data, str):
            self.write(data)
        elif isinstance(data, dict):
            self.write(json.dumps(data))

class InputValidationError(Exception):
    def __init__(self, message=None):
        super(InputValidationError, self).__init__()
        self.message = message