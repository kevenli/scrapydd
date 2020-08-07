from .base import RestBaseHandler
from tornado.web import Application


class NodesHandler(RestBaseHandler):
    """
    Register a node.
    A user (admin) can request a token from the server. (key, secret_key pair)
    By this token, a node can be registered to the server.

    The node is communicating with server by http/https at the
    same port of ui now. There will be a grpc server in the future.
    A node register to server by communicate to the api port, the api
    return necessary info for the node to run. after the registration
    it communicate to the server only on grpc endpoint.
    """
    def post(self):
        pass


def apply(app: Application):
    app.add_handlers(".*", [
        ('/v1/nodes', NodesHandler),
    ])
