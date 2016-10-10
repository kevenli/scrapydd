from models import Node, Session
from tornado.ioloop import IOLoop, PeriodicCallback
import datetime
import logging
from .exceptions import *

class NodeManager():
    interval = 10
    node_timeout = 60

    def __init__(self, scheduler_manager):
        self.scheduler_manager = scheduler_manager

    def init(self):
        self.ioloop = IOLoop.current()
        self.peroid_callback = PeriodicCallback(self._poll, self.interval*1000, self.ioloop)
        self.peroid_callback.start()

    def _poll(self):
        last_heartbeat_lessthan = datetime.datetime.now() - datetime.timedelta(seconds=self.node_timeout)
        session = Session()
        for node in session.query(Node).filter(Node.last_heartbeat<last_heartbeat_lessthan, Node.isalive==1):
            logging.info('node %d expired' % node.id)
            self.scheduler_manager.on_node_expired(node.id)
            node.isalive = 0
            session.add(node)
        session.commit()
        session.close()

        logging.debug('node manager poll')

    def heartbeat(self, node_id):
        try:
            session = Session()
            node = session.query(Node).filter_by(id=node_id).first()
            if node.isalive == 0:
                raise NodeExpired()
            node.last_heartbeat = datetime.datetime.now()
            session.add(node)
            session.commit()
        finally:
            session.close()

    def create_node(self, remote_ip):
        session = Session()
        node = Node()
        node.client_ip = remote_ip
        node.create_time = datetime.datetime.now()
        node.last_heartbeat = datetime.datetime.now()
        node.isalive = 1
        session.add(node)
        session.commit()
        session.refresh(node)
        session.close()
        return node
