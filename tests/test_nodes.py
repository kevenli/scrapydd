import unittest
from scrapydd.nodes import NodeManager, AnonymousNodeDisabled
from scrapydd.models import init_database, session_scope, Session
from scrapydd.config import Config


class NodeManagerTest(unittest.TestCase):
    def setUp(self) -> None:
        config = Config(values={'database_url': 'sqlite:///test.db'})
        init_database(config)

    def test_node_online(self):
        target = NodeManager(None)
        node_id = None
        client_ip = '127.0.0.2'
        tags = None
        with session_scope() as session:
            node = target.node_online(session, node_id, client_ip, tags)
        self.assertIsNotNone(node.id)
        self.assertIsNotNone(node.name)
        self.assertEqual(node.client_ip, client_ip)
        self.assertEqual(node.tags, tags)

    def test_node_online_authentication(self):
        target = NodeManager(None, enable_authentication=True)
        node_id = None
        client_ip = '127.0.0.2'
        tags = None
        with session_scope() as session:
            try:
                node = target.node_online(session, node_id, client_ip, tags)
                self.fail('No exception caught')
            except AnonymousNodeDisabled:
                pass

    def test_create_node_session(self):
        target = NodeManager(None)
        session = Session()
        node_session = target.create_node_session(session)
        self.assertIsNotNone(node_session)


class AuthenticatedNodeManager(unittest.TestCase):
    def setUp(self) -> None:
        config = Config(values={'database_url': 'sqlite:///test.db'})
        init_database(config)

    def get_target(self):
        target = NodeManager(None, enable_authentication=True)
        return target

    def test_create_node_session(self):
        target = self.get_target()
        session = Session()
        try:
            node_session = target.create_node_session(session)
            self.fail('Exception not caught')
        except Exception:
            pass
