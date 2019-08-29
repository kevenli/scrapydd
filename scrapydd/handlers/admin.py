from .base import AppBaseHandler
from ..models import *
from tornado.web import authenticated
from datetime import datetime, timedelta
from ..security import generate_random_string
import uuid
from sqlalchemy import or_


class AdminNodesHandler(AppBaseHandler):
    @authenticated
    def get(self):
        if not self.current_user or not self.current_user.is_admin:
            return self.set_status(403, reason="No permission")
        with session_scope() as session:
            nodes = list(session.query(Node).filter(or_(Node.node_key_id.isnot(None),Node.is_deleted.is_(False))))
            usable_key = self._get_new_node_key()
            self.render('admin/nodes.html', nodes=nodes, usable_key=usable_key)

    def _get_new_node_key(self):
        with session_scope() as session:
            new_node_key_expire = datetime.now() - timedelta(hours=1)
            unused_node_keys = session.query(NodeKey).filter(NodeKey.used_node_id.is_(None),
                                                             NodeKey.is_deleted == False).all()
            usable_key = None
            for unused_key in unused_node_keys:
                if unused_key.create_at < new_node_key_expire:
                    unused_key.is_deleted = True
                    session.add(unused_key)
                else:
                    usable_key = unused_key
                    break

            if usable_key is None:
                usable_key = NodeKey()
                usable_key.key = str(uuid.uuid4())
                usable_key.create_at = datetime.now()
                usable_key.secret_key = generate_random_string(32)
                session.add(usable_key)
            session.commit()
            return usable_key


class AdminHomeHandler(AppBaseHandler):
    @authenticated
    def get(self):
        if not self.current_user or not self.current_user.is_admin:
            return self.set_status(403, "No permission")

        with session_scope() as session:
            user_count = session.query(User).count()
            active_node_count = session.query(Node).filter_by(isalive=True).count()
            all_node_count = session.query(Node).count()
            self.render('admin/home.html',
                        user_count = user_count,
                        active_node_count=active_node_count,
                        all_node_count=all_node_count)