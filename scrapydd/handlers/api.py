import datetime
import json
import logging
from tornado.web import Application
from .node import NodeHmacAuthenticationProvider, NodeBaseHandler


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
        enable_node_registration = self.settings.get('enable_node_registration'
                                                     , False)
        node_id = self.current_user

        # if node_registerion is enabled, node should be assigned
        # node_id before use by
        # invoke /nodes/register
        logger.debug('enable_node_registration : %s', enable_node_registration)
        if enable_node_registration and node_id is None:
            return self.set_status(403, 'node registration is enabled, '
                                        'no key provided.')

        tags = self.get_argument('tags', '').strip()
        tags = None if tags == '' else tags
        remote_ip = self.request.headers.get('X-Real-IP',
                                             self.request.remote_ip)
        node_manager = self.settings.get('node_manager')
        if node_id:
            node = node_manager.get_node(node_id)
            session = self.session
            node.tags = tags
            node.isalive = True
            node.client_ip = remote_ip
            node.last_heartbeat = datetime.datetime.now()
            session.add(node)
            session.commit()
        else:
            node = node_manager.create_node(remote_ip, tags=tags)
        with open('keys/localhost.crt', 'r') as f:
            cert_text = f.read()
        return self.write(json.dumps({
            'id': node.id,
            'cert': cert_text,
        }))


def apply(app: Application):
    app.add_handlers(".*", [
        ('/v1/nodes', NodesHandler),
    ])
