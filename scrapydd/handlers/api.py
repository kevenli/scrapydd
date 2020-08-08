import datetime
import json
import logging
from tornado.web import Application
from .node import NodeHmacAuthenticationProvider, NodeBaseHandler
from ..nodes import AnonymousNodeDisabled


logger = logging.getLogger(__name__)


class NodesHandler(NodeBaseHandler):
    """
    Online a node.
    A user (admin) can request a token from the server. (key, secret_key pair)
    By this token, a node can be registered to the server.

    The node is communicating with server by http/https at the
    same port of ui now. There will be a grpc server in the future.
    A node register to server by communicate to the api port, the api
    return necessary info for the node to run. after the registration
    it communicate to the server only on grpc endpoint.

    A node should always call this api before further running. This make
    authentication of node and provide the lastest server certs, node token,
    grpc endpoint information and so on.

    There are two types of node, PERMANENT/TEMPORARY.
    A permanent node have to be registered explicitly, it will still be
    shown on the nodes page even if it is not online, always be the same
    node_id.
    A temporary node will be assigned a new node_id each time it is online.
    It is not registered explicitly. This mode can only work when
    `enable_node_registration` config is set to false.
    Whichever the node type it belongs, the node have to invoke this api
    to be online.
    """
    def post(self):
        node_id = self.current_user
        tags = self.get_argument('tags', '').strip()
        tags = None if tags == '' else tags
        remote_ip = self.request.headers.get('X-Real-IP',
                                             self.request.remote_ip)
        try:
            node = self.node_manager.node_online(self.session, node_id, remote_ip,
                                                 tags)
            with open('keys/localhost.crt', 'r') as f:
                cert_text = f.read()
            return self.write(json.dumps({
                'id': node.id,
                'serverCert': cert_text,
            }))
        except AnonymousNodeDisabled:
            return self.set_status(403, 'AnonymousNodeDisabled')


def apply(app: Application):
    app.add_handlers(".*", [
        ('/v1/nodes', NodesHandler),
    ])
