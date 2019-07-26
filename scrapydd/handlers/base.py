import tornado.web
import os

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')


class AppBaseHandler(tornado.web.RequestHandler):
    authentication_provider = None

    def initialize(self):
        self.authentication_provider = self.settings.get('authentication_provider')

    def get_current_user(self):
        return self.authentication_provider.get_user(self)

    def data_received(self, chunk):
        pass

    @property
    def template_loader(self):
        loader = tornado.template.Loader(BASE_DIR)
        return loader


class RestBaseHandler(AppBaseHandler):
    def check_xsrf_cookie(self):
        return None