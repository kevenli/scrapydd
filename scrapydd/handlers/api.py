from .base import RestBaseHandler
from tornado.web import Application


class NodesHandler(RestBaseHandler):
    def post(self):
        pass


def apply(app: Application):
    app.add_handlers(".*", [
        ('/v1/nodes', NodesHandler),
    ])
