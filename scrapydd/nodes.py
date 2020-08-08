import datetime
import uuid
import logging
import namegenerator
from tornado.ioloop import IOLoop, PeriodicCallback
from .exceptions import *
from .models import Node, Session, session_scope, NodeKey
from .security import generate_random_string
from .utils.snowflake import generator

logger = logging.getLogger(__name__)


class AnonymousNodeDisabled(Exception):
    """
    If enable_authentication is set to true, anonymous node
    is not allowed.
    """
    pass


class NodeManager():
    interval = 10
    node_timeout = 60

    def __init__(self, scheduler_manager, enable_authentication=False):
        self.scheduler_manager = scheduler_manager
        self.id_generator = generator(1, 1)
        self._enable_authentication = enable_authentication

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

    def create_node(self, remote_ip, tags=None, key_id=None, name=None):
        with session_scope() as session:
            node = Node()
            node.id = next(self.id_generator)
            node.client_ip = remote_ip
            node.create_time = datetime.datetime.now()
            node.last_heartbeat = datetime.datetime.now()
            node.isalive = True
            node.tags = tags
            node.node_key_id = key_id
            if not name:
                name = namegenerator.gen()
            node.name = name
            session.add(node)
            session.commit()
            session.refresh(node)
            return node

    def node_online(self, session, node_id, remote_ip, tags):
        if node_id is None and self._enable_authentication:
            raise AnonymousNodeDisabled()

        if node_id is None:
            node = self.create_node(remote_ip, tags)
        else:
            node = self._get_node(session, node_id)
            # version compatible, old version have no name
            # create one for it
            if not node.name:
                node.name = namegenerator.gen()
            node.client_ip = remote_ip
            session.add(node)
            session.commit()
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

    def _get_node(self, session, node_id):
        return session.query(Node).get(node_id)

    def get_node(self, node_id):
        with session_scope() as session:
            return session.query(Node).get(node_id)

    def update_node_use_key(self, node_key, node_id):
        with session_scope() as session:
            node_key.used_node_id = node_id
            session.add(node_key)
            session.commit()
