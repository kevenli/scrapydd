from tests.base import AppTest, SecureAppTest
from scrapydd.models import session_scope, NodeKey

class AdminNodesHandlerTest(AppTest):
    def test_get(self):
        response = self.fetch('/admin/nodes')
        self.assertEqual(200, response.code)

    def test_node_creation(self):
        with session_scope() as session:
            session.query(NodeKey).delete()
            session.commit()

            self.assertEqual(0, len(session.query(NodeKey).all()))
            response = self.fetch('/admin/nodes')
            self.assertEqual(200, response.code)

            self.assertEqual(1, len(session.query(NodeKey).all()))

            usable_key = session.query(NodeKey).filter(NodeKey.used_node_id.is_(None),
                                                             NodeKey.is_deleted == False).first()

            self.assertEqual(False, usable_key.is_deleted)
            self.assertIsNone(usable_key.used_node_id)

class AdminNodeHandlerSecureTest(SecureAppTest):
    def test_get(self):
        headers = {}
        headers = self.populate_cookie_header(headers)
        response = self.fetch('/admin/nodes', headers=headers, follow_redirects=False)
        self.assertEqual(200, response.code)

    def test_without_auth(self):
        response = self.fetch('/admin/nodes', follow_redirects=False)
        self.assertIn(response.code, [403, 302])

    def test_no_permission(self):
        headers = self.populate_cookie_header(headers={}, username='adam')
        response = self.fetch('/admin/nodes', follow_redirects=False, headers=headers)

        self.assertEqual(403, response.code)