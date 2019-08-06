from .base import AppBaseHandler
from ..models import UserKey, session_scope
from tornado.web import authenticated
import datetime
import random
import string


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
