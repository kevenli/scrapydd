from tornado.web import authenticated, HTTPError
from datetime import datetime, timedelta
import uuid
from distutils.util import strtobool
from sqlalchemy import or_
from .base import AppBaseHandler
from ..models import *
from .. import models
from ..security import generate_random_string, encrypt_password
from .auth import admin_required


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
                        user_count=user_count,
                        active_node_count=active_node_count,
                        all_node_count=all_node_count)


class AdminPluginsHandler(AppBaseHandler):
    @authenticated
    def get(self):
        if not self.current_user.is_admin:
            return self.set_status(403, "No permission")

        spider_plugin_manager = self.settings.get('spider_plugin_manager')
        plugins = spider_plugin_manager.get_all_plugins()
        self.render('admin/spiderplugins.html', plugins=plugins)


class AdminUsersHandler(AppBaseHandler):
    @authenticated
    def get(self):
        if not self.current_user.is_admin:
            return self.set_status(403, "No permission")

        session = self.session
        users = session.query(User).all()
        self.render('admin/users.html', users=users)


class AdminDisableUserAjaxHandler(AppBaseHandler):
    @authenticated
    def post(self):
        if not self.current_user.is_admin:
            return self.set_status(403, "No permission")

        target_user_id = int(self.get_body_argument('userid'))
        if target_user_id == self.current_user.id:
            return self.set_status(400, 'Cannot modify current user status')
        session = self.session
        user = session.query(User).get(target_user_id)
        user.is_active = False
        session.add(user)
        session.commit()


class AdminEnableUserAjaxHandler(AppBaseHandler):
    @authenticated
    def post(self):
        if not self.current_user.is_admin:
            return self.set_status(403, "No permission")

        target_user_id = int(self.get_body_argument('userid'))
        if target_user_id == self.current_user.id:
            return self.set_status(400, 'Cannot modify current user status')
        session = self.session
        user = session.query(User).get(target_user_id)
        user.is_active = True
        session.add(user)
        session.commit()


class AdminNewUserHandler(AppBaseHandler):
    @admin_required
    def get(self):
        return self.render('admin/new_user.html')


    @admin_required
    def post(self):
        username = self.get_body_argument('username')
        password = self.get_body_argument('password')
        email = self.get_body_argument('email')
        password2 = self.get_body_argument('password2')
        is_active = strtobool(self.get_body_argument('is_active', 'true'))
        is_admin = strtobool(self.get_body_argument('is_admin', 'false'))

        session = self.session

        if not username:
            raise HTTPError(400, 'username is required.')

        existing_user = session.query(User).filter_by(username=username).first()
        if existing_user:
            raise HTTPError(400, 'username already exists.')

        if not email:
            raise HTTPError(400, 'email is required.')

        if not password:
            raise HTTPError(400, 'password is required')

        if password != password2:
            raise HTTPError(400, 'please input identical passwords.')

        entrypted_password = encrypt_password(password, self.settings.get('cookie_secret', ''))

        user = models.User()
        user.username = username
        user.email = email
        user.password = entrypted_password
        user.is_active = is_active
        user.is_admin = is_admin
        session.add(user)
        session.commit()
        return self.redirect('/admin/users')
