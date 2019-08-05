from .base import AppBaseHandler
from ..models import session_scope, Node


class AdminNodesHandler(AppBaseHandler):
    def get(self):
        if not self.current_user or not self.current_user.is_admin:
            return self.write_error(403, "No permission")
        with session_scope() as session:
            nodes = list(session.query(Node))
            self.render('nodes.html', nodes=nodes)