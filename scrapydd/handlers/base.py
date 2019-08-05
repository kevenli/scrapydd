import tornado.web
import tornado.template
import os
from ..models import session_scope, User

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')


class AppBaseHandler(tornado.web.RequestHandler):
    authentication_provider = None

    def initialize(self):
        self.authentication_provider = self.settings.get('authentication_provider')

    def get_current_user(self):
        username = self.authentication_provider.get_user(self)
        with session_scope() as session:
            return session.query(User).filter_by(username=username).first()

    def data_received(self, chunk):
        pass


class RestBaseHandler(AppBaseHandler):
    def check_xsrf_cookie(self):
        return None