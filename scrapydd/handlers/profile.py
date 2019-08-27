from .base import AppBaseHandler, InputValidationError
from ..models import UserKey, session_scope, User
from tornado.web import authenticated
import datetime
import random
import string
import logging
from ..security import encrypt_password

logger = logging.getLogger(__name__)


def generate_random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))


class ProfileHomeHandler(AppBaseHandler):
    @authenticated
    def get(self):
        self.render('profile/profile.html')


class ProfileKeysHandler(AppBaseHandler):
    @authenticated
    def get(self):
        with session_scope() as session:
            user_keys = list(session.query(UserKey).filter_by(user_id=self.current_user.id))
            self.render('profile/profile_keys.html', user_keys=user_keys)

    @authenticated
    def post(self, *args, **kwargs):
        with session_scope() as session:
            if session.query(UserKey).filter_by(user_id=self.current_user.id).count() >= 10:
                return self.set_status(400, reason='user can only host at most 10 keys.')
            new_appkey = generate_random_string(12)
            while session.query(UserKey).filter_by(app_key=new_appkey).first():
                new_appkey = generate_random_string(12)
            new_appsecret = generate_random_string(20)

            new_key = UserKey(user_id=self.current_user.id, app_key=new_appkey, app_secret=new_appsecret,
                              create_at=datetime.datetime.now())
            session.add(new_key)
            session.commit()

            self.render('profile/profile_newkey.html', new_key=new_key)


class ProfileChangepasswordHandler(AppBaseHandler):
    @authenticated
    def get(self):
        return self.render('profile/change_password.html')

    @authenticated
    def post(self):
        try:
            old_password = self.get_body_argument('oldpassword')

            new_password = self.get_body_argument('newpassword')
            new_password2 = self.get_body_argument('newpassword2')
            if not old_password:
                raise InputValidationError("Please input password")

            encrypted_password = encrypt_password(old_password, self.settings.get('cookie_secret', ''))
            if encrypted_password != self.current_user.password:
                raise InputValidationError("Invalid password.")

            if not new_password:
                raise InputValidationError("Please input a new password.")

            if not new_password2:
                raise InputValidationError("Please confirm the new password.")

            new_password = new_password.strip()

            if new_password != new_password2:
                raise InputValidationError("Please confirm the new password.")

            encrypted_new_password = encrypt_password(new_password, self.settings.get('cookie_secret', ''))
            with session_scope() as session:
                user = session.query(User).filter_by(id=self.current_user.id).first()
                user.password =encrypted_new_password
                session.add(user)
                session.commit()

            self.render('profile/change_password_success.html')

        except InputValidationError as e:
            self.render('profile/change_password.html', error_message=e.message)