import datetime
import uuid
import logging
import namegenerator
from tornado.ioloop import IOLoop, PeriodicCallback
from .exceptions import *
from .models import Node, Session, session_scope, NodeKey, NodeSession
from .security import generate_random_string
from .utils.snowflake import generator

logger = logging.getLogger(__name__)


class AnonymousNodeDisabled(Exception):
    """
    If enable_authentication is set to true, anonymous node
    is not allowed.
    """
    pass


class NodeNotFoundException(Exception):
    pass


class NodeKeyNotFoundException(Exception):
    pass


class LivingNodeSessionExistException(Exception):
    """
    Raise there is another existing NodeSession
    relates to the target Node.
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

        self._clean_expired_node_session(session)
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

    def get_node(self, node_id, session=None):
        with session_scope() as session:
            return session.query(Node).get(node_id)

    def update_node_use_key(self, node_key, node_id):
        with session_scope() as session:
            node_key.used_node_id = node_id
            session.add(node_key)
            session.commit()

    def _new_id(self):
        return next(self.id_generator)

    def _solve_node_id(self, node):
        """
            To provide compatibility that web handler layer
            passed node context into actions can be either Node
            or int value of node.id, use this method to convert
            it first to get a actual node id.
        :param node: [Node, int]
        :return: [int] node id
        """
        if not node:
            return None

        if isinstance(node, Node):
            return node.id

        if isinstance(node, int):
            return node

        raise Exception('Not supported type %s', type(node))

    def create_node_session(self, session, node=None,
                            client_ip=None, tags=None):
        node_id = self._solve_node_id(node)

        if node_id is None and self._enable_authentication:
            raise AnonymousNodeDisabled()

        if node_id is None:
            node = self.create_node(client_ip, tags)
        else:
            node = session.query(Node).get(node_id)
            if not node:
                raise NodeNotFoundException()

        existing_session = session.query(NodeSession)\
            .filter_by(node_id=node.id).first()

        # latest_existing_session_kickout_time
        # A session cannot be kickout if just logged in in a minutes.
        leskt = datetime.datetime.now() - datetime.timedelta(minutes=1)
        if existing_session and existing_session.create_at > leskt:
            raise LivingNodeSessionExistException()

        # otherwise, remove that session.
        if existing_session:
            session.delete(existing_session)
            session.flush()

        new_session = NodeSession(id=self._new_id(),
                                  node_id=node.id,
                                  create_at=datetime.datetime.now(),
                                  last_heartbeat=datetime.datetime.now())
        session.add(new_session)
        session.commit()
        logger.info('NodeSession %s created.', new_session.id)
        return new_session

    def _clean_expired_node_session(self, session):
        last_heartbeat_lessthan = datetime.datetime.now() \
                              - datetime.timedelta(seconds=self.node_timeout)

        for node_session in session.query(NodeSession).all():
            if node_session.last_heartbeat < last_heartbeat_lessthan:
                logger.info('NodeSession %s is timeout, deleting.', node_session.id)
                session.delete(node_session)

        session.commit()

    def node_session_heartbeat(self, session, node_session_id):
        node_session = session.query(NodeSession).get(node_session_id)
        if node_session is None:
            raise NodeExpired()

        node = node_session.node
        if not node:
            raise NodeExpired()

        if node.isalive == 0 and not node.node_key_id:
            raise NodeExpired()

        node.last_heartbeat = datetime.datetime.now()
        node_session.last_heartbeat = datetime.datetime.now()
        node.isalive = True
        session.add(node_session)
        session.add(node)
        session.commit()
        return node_session

    def get_node_session(self, session, node_session_id, node=None):
        node_id = self._solve_node_id(node)
        node_session = session.query(NodeSession).get(node_session_id)
        if not node_session:
            return None

        if node_id and node_session.node_id != node_id:
            return None

        return node_session

    def node_has_task(self, session, node_id):
        return self.scheduler_manager.has_task(node_id)

    def jobs_running(self, session, node_id, running_job_ids):
        return self.scheduler_manager.jobs_running(node_id, running_job_ids)

    def get_node_by_token(self, session, token):
        key = session.query(NodeKey).filter_by(key=token).first()
        if not key:
            raise NodeKeyNotFoundException()

        if key.used_node_id is None:
            return None

        node = session.query(Node).get(key.used_node_id)
        if node.is_deleted:
            return None
        return node

    def node_get_next_job(self, session, node):
        return self.scheduler_manager.get_next_task(node.id)

    def job_finish(self, session, job, status, log_file=None, items_file=None):
        job.status = status
        return self.scheduler_manager.job_finished(job, log_file=log_file,
                                            items_file=items_file)