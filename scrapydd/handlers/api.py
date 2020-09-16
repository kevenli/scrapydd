import json
import logging
from tornado.web import Application, MissingArgumentError, authenticated
from .base import RestBaseHandler


logger = logging.getLogger(__name__)


class ApiHandler(RestBaseHandler):
    def _is_json_request(self):
        content_type = self.request.headers.get('Content-Type')
        if not content_type:
            return False

        return content_type == 'application/json'

    """Request handler where requests and responses speak JSON."""
    def prepare(self):
        # Incorporate request JSON into arguments dictionary.
        if self.request.body and self._is_json_request():
            try:
                json_data = json.loads(self.request.body)
                for k, v in json_data.items():
                    self.request.arguments[k] = [v]
            except ValueError:
                message = 'Unable to parse JSON.'
                self.send_error(400, message=message) # Bad Request

        # Set up response dictionary.
        self.response = dict()

    def set_default_headers(self):
        self.set_header('Content-Type', 'application/json')

    def write_error(self, status_code, errcode=None, errmsg=None, **kwargs):
        if 'message' not in kwargs:
            if status_code == 405:
                errmsg = 'Invalid HTTP method.'
        if not errmsg:
            errmsg = 'Unknown Error'
        if not errcode:
            errcode = 999

        self.response = dict({
            'errmsg': errmsg,
            'errcode': errcode
        })
        self.set_status(status_code)
        self.write_json()

    def write_json(self):
        output = json.dumps(self.response)
        self.write(output)


class ProjectsHandler(ApiHandler):
    @authenticated
    def post(self):
        try:
            project_name = self.get_argument('name')
        except MissingArgumentError:
            return self.write_error(400, errmsg='Parameter name required.')
        project = self.project_manager.create_project(self.session,
                                                      self.current_user,
                                                      project_name,
                                                      return_existing=True)

        self.response['id'] = project.id
        self.response['name'] = project.name
        self.write_json()


def apply(app: Application):
    app.add_handlers(".*", [
        ('/v1/projects', ProjectsHandler),
    ])
