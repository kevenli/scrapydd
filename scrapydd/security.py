import functools
from .models import User
from six.moves.urllib.parse import urlsplit, urlencode
from tornado.web import HTTPError
import json

def signin_view(func):
    pass

def authentication_check(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.current_user:
            if self.request.method in ("GET", "HEAD"):
                url = self.get_login_url()
                if "?" not in url:
                    if urlsplit(url).scheme:
                        # if login url is absolute, make next absolute too
                        next_url = self.request.full_url()
                    else:
                        assert self.request.uri is not None
                        next_url = self.request.uri
                    url += "?" + urlencode(dict(next=next_url))
                self.redirect(url)
                return None
            raise HTTPError(403)
        return method(self, *args, **kwargs)
    return wrapper


class AuthenticationProvider(object):
    def get_user(self, handler):
        raise NotImplementedError()


class NoAuthenticationProvider(object):
    def get_user(self, handler):
        return 'admin'


class CookieAuthenticationProvider(object):
    def get_user(self, handler):
        user_cookie = handler.get_secure_cookie("user")
        if user_cookie:
            #return json.loads(user_cookie)
            return user_cookie
        return None

