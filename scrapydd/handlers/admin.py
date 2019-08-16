from .base import AppBaseHandler
from ..models import *
from tornado.web import authenticated

class AdminNodesHandler(AppBaseHandler):
    @authenticated
    def get(self):
        if not self.current_user or not self.current_user.is_admin:
            return self.write_error(403, "No permission")
        with session_scope() as session:
            nodes = list(session.query(Node))
            self.render('admin/nodes.html', nodes=nodes)


class AdminHomeHandler(AppBaseHandler):
    @authenticated
    def get(self):
        with session_scope() as session:
            user_count = session.query(User).count()
            self.render('admin/home.html', user_count = user_count)