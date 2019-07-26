import tornado.web
import json
from .handlers.base import RestBaseHandler


class RestNodesHandler(RestBaseHandler):
    def initialize(self):
        self.node_manager = self.settings.get('node_manager')

    def post(self):
        tags = self.get_argument('tags', '').strip()
        tags = None if tags == '' else tags
        remote_ip = self.request.remote_ip
        node = self.node_manager.create_node(remote_ip, tags=tags)
        self.write(json.dumps({'id': node.id}))