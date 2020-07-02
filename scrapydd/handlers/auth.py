from tornado.web import authenticated, HTTPError, RequestHandler
from .base import AppBaseHandler
from ..models import session_scope, User
from ..security import encrypt_password



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

            encrypted_password = encrypt_password(password, self.settings.get('cookie_secret', ''))

            if user.password != encrypted_password:
                return self.get()

            self.set_secure_cookie("user", username)
            self.redirect(self.get_query_argument('next', '/'))


class LogoutHandler(AppBaseHandler):
    def get(self):
        self.clear_cookie('user')
        self.redirect('/')


def admin_required(func):
    def wrapper(self: RequestHandler, *args, **kwargs):
        user = self.current_user
        if not user or not user.is_admin:
            raise HTTPError(403)

        return func(self, *args, **kwargs)

    return wrapper
