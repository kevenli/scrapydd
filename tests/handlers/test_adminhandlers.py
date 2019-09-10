from tests.base import AppTest, SecureAppTest
from scrapydd.models import session_scope, NodeKey
import datetime
import logging

logger = logging.getLogger(__name__)

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
        logger.debug(headers)
        response = self.fetch('/admin/nodes', headers=headers, follow_redirects=False)
        self.assertEqual(200, response.code)

    def test_without_auth(self):
        response = self.fetch('/admin/nodes', follow_redirects=False)
        self.assertIn(response.code, [403, 302])

    def test_no_permission(self):
        headers = self.populate_cookie_header(headers={}, username='adam')
        response = self.fetch('/admin/nodes', follow_redirects=False, headers=headers)

        self.assertEqual(403, response.code)

    def test_get_key_expire(self):
        with session_scope() as session:
            session.query(NodeKey).delete()

            expire_key = NodeKey()
            expire_key.key = 'abc'
            expire_key.secret_key = 'cba'
            expire_key.is_deleted = False
            expire_key.create_at = datetime.datetime.now() - datetime.timedelta(hours=2)
            session.add(expire_key)
            session.commit()
            session.expire_all()

            headers = self.populate_cookie_header(headers={})
            response = self.fetch('/admin/nodes', headers=headers)
            self.assertEqual(200, response.code)

            expire_key = session.query(NodeKey).filter_by(key='abc').first()
            self.assertTrue(expire_key.is_deleted)

            new_key = session.query(NodeKey).filter_by(is_deleted=False).first()
            self.assertIsNotNone(new_key)


class AdminHomeHandlerTest(SecureAppTest):
    def test_get(self):
        headers = {}
        headers = self.populate_cookie_header(headers)
        response = self.fetch('/admin', headers=headers, follow_redirects=False)
        self.assertEqual(200, response.code)

    def test_get_no_permission(self):
        headers = self.populate_cookie_header(headers={}, username='adam')
        response = self.fetch('/admin', follow_redirects=False, headers=headers)
        self.assertEqual(403, response.code)

    def test_get_no_auth(self):
        response = self.fetch('/admin/nodes', follow_redirects=False)
        self.assertEqual(302, response.code)