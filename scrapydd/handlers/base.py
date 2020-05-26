# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
import logging
import json
import os
import datetime
import uuid
import decimal
import tornado.web
import tornado.template
from scrapydd.models import session_scope, User, Project, Spider, Session
from scrapydd.security import CookieAuthenticationProvider, HmacAuthorize
from scrapydd.security import BasicAuthentication
from scrapydd.exceptions import ProjectNotFound, SpiderNotFound
from scrapydd.project import ProjectManager

LOGGER = logging.getLogger(__name__)

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')


class AppBaseHandler(tornado.web.RequestHandler):
    authentication_providers = [CookieAuthenticationProvider()]
    _session = None

    def get_default_user(self):
        return self.session.query(User).get(1)

    @property
    def session(self):
        if self._session is None:
            self._session = Session()

        return self._session

    def get_current_user(self):
        if not self.settings.get('enable_authentication', False):
            return self.get_default_user()

        username = None
        for authentication_provider in self.authentication_providers:
            username = authentication_provider.get_user(self)
            if username:
                break

        if not username:
            return None

        return self.session.query(User).filter_by(username=username).first()

    def data_received(self, chunk):
        pass

    def build_runner(self, eggf):
        factory = self.settings.get('runner_factory')
        return factory.build(eggf)

    def get_spider(self, session, project_name, spider_name):
        project = session.query(Project) \
            .filter_by(name=project_name).first()
        if not project:
            raise ProjectNotFound()

        spider = session.query(Spider) \
            .filter_by(project_id=project.id, name=spider_name).first()
        if not spider:
            raise SpiderNotFound()
        return spider

    @property
    def project_manager(self) -> ProjectManager:
        return self.application.settings.get('project_manager')

    def on_finish(self):
        if self._session:
            self._session.commit()
            self._session.close()
            self._session = None


class RestJsonEncoder(json.JSONEncoder):
    """
    JSONEncoder subclass that knows how to encode date/time, decimal types, and
    UUIDs.
    """
    def default(self, o):
        # See "Date Time String Format" in the ECMA-262 specification.
        if isinstance(o, datetime.datetime):
            r = o.isoformat()
            if o.microsecond:
                r = r[:23] + r[26:]
            if r.endswith('+00:00'):
                r = r[:-6] + 'Z'
            return r
        elif isinstance(o, datetime.date):
            return o.isoformat()
        elif isinstance(o, datetime.time):
            r = o.isoformat()
            if o.microsecond:
                r = r[:12]
            return r
        elif isinstance(o, (decimal.Decimal, uuid.UUID)):
            return str(o)
        else:
            return super().default(o)


class RestBaseHandler(AppBaseHandler):
    authentication_providers = [HmacAuthorize(), BasicAuthentication()]

    def check_xsrf_cookie(self):
        return None

    def send_json(self, data):
        self.set_header('Content-type', 'application/json')
        if isinstance(data, str):
            self.write(data)
        elif isinstance(data, dict):
            self.write(json.dumps(data, cls=RestJsonEncoder))


class InputValidationError(Exception):
    def __init__(self, message=None):
        super(InputValidationError, self).__init__()
        self.message = message
