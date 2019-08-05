import hashlib
from .base import AppBaseHandler
from ..models import session_scope, User


class SignupHandler(AppBaseHandler):
    def get(self):
        pass

    def post(self):
        pass


class SigninHandler(AppBaseHandler):
    def get(self):
        self.render('signin.html')

    def post(self):
        username = self.get_body_argument('username')
        password = self.get_body_argument('password')

        with session_scope() as session:
            user = session.query(User).filter_by(username=username).first()
            if user is None:
                return self.get()

            m = hashlib.md5()
            m.update(password)
            encrypted_password = m.hexdigest()

            if user.password != encrypted_password:
                return self.get()

            self.set_secure_cookie("user", username)
            self.redirect(self.get_query_argument('next', '/'))


class LogoutHandler(AppBaseHandler):
    def get(self):
        self.clear_cookie('user')
        self.redirect('/')
