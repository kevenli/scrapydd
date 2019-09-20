from .models import Node, Session, session_scope, NodeKey
from tornado.ioloop import IOLoop, PeriodicCallback
import datetime
import logging
from .exceptions import *
import uuid
from .security import generate_random_string

logger = logging.getLogger(__name__)

class NodeManager():
    interval = 10
    node_timeout = 60

    def __init__(self, scheduler_manager):
        self.scheduler_manager = scheduler_manager

    def init(self):
        self.ioloop = IOLoop.current()
        self.peroid_callback = PeriodicCallback(self._poll, self.interval*1000)
        self.peroid_callback.start()

    def _poll(self):
        last_heartbeat_lessthan = datetime.datetime.now() - datetime.timedelta(seconds=self.node_timeout)
        session = Session()
        for node in session.query(Node).filter(Node.last_heartbeat<last_heartbeat_lessthan, Node.isalive==1):
            logger.info('node %d expired' % node.id)
            self.scheduler_manager.on_node_expired(node.id)
            node.isalive = 0
            if not node.node_key_id:
                node.is_deleted = True
            session.add(node)
        session.commit()
        session.close()

        logger.debug('node manager poll')

    def heartbeat(self, node_id):
        try:
            session = Session()
            node = session.query(Node).filter_by(id=node_id).first()
            if node is None:
                raise NodeExpired()
            if node.isalive == 0 and not node.node_key_id:
                raise NodeExpired()
            node.last_heartbeat = datetime.datetime.now()
            node.isalive = True
            session.add(node)
            session.commit()
        finally:
            session.close()

    def create_node(self, remote_ip, tags=None, key_id=None):
        with session_scope() as session:
            node = Node()
            node.client_ip = remote_ip
            node.create_time = datetime.datetime.now()
            node.last_heartbeat = datetime.datetime.now()
            node.isalive = True
            node.tags = tags
            node.node_key_id = key_id
            session.add(node)
            session.commit()
            session.refresh(node)
            return node

    def create_node_key(self):
        with session_scope() as session:
            node_key = NodeKey()
            node_key.key = str(uuid.uuid4())
            node_key.create_at = datetime.datetime.now()
            node_key.secret_key = generate_random_string(32)
            session.add(node_key)
            session.commit()
            session.refresh(node_key)
            return node_key

    def get_node(self, node_id):
        with session_scope() as session:
            return session.query(Node).get(node_id)

    def update_node_use_key(self, node_key, node_id):
        with session_scope() as session:
            node_key.used_node_id = node_id
            session.add(node_key)
            session.commit()
