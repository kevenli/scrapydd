from models import Node, Session
from tornado.ioloop import IOLoop, PeriodicCallback
import datetime

class NodeManager():
    interval = 10
    node_timeout = 60
    def init(self):
        self.ioloop = IOLoop.current()
        self.peroid_callback = PeriodicCallback(self._poll, self.interval*1000, self.ioloop)
        self.peroid_callback.start()

    def _poll(self):
        last_heartbeat_lessthan = datetime.datetime.now() - datetime.timedelta(seconds=self.node_timeout)
        session = Session()
        for node in session.query(Node).filter(Node.last_heartbeat<last_heartbeat_lessthan):
            print 'node %d expired' % node.id
            session.delete(node)
        session.commit()
        session.close()

        print 'node manager poll'

    def create_node(self, remote_ip):
        session = Session()
        node = Node()
        node.client_ip = remote_ip
        node.create_time = datetime.datetime.now()
        node.last_heartbeat = datetime.datetime.now()
        session.add(node)
        session.commit()
        session.refresh(node)
        session.close()
        return node
