from ..base import AppTest
from scrapydd.storage import ProjectStorage
from scrapydd.models import session_scope, Project, Spider
from os import path
from six.moves.urllib.parse import urlencode

class TestDeleteProjectHandler(AppTest):
    def test_post(self):
        project_name = 'test_project'
        self._upload_test_project()
        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()

            project_storage = ProjectStorage(self._app.settings.get('project_storage_dir'), project)

            self.assertTrue(path.exists(project_storage.storage_provider.get_project_eggs_dir(project)))

            headers = {'Cookie': "_xsrf=dummy"}
            post_data = {'_xsrf': 'dummy'}
            res = self.fetch('/projects/%s/delete' % project_name, method="POST", headers=headers,
                             body=urlencode(post_data))
            self.assertEqual(200, res.code)

            # do not delete folder
            # self.assertFalse(path.exists(project_storage.storage_provider.get_project_eggs_dir(project)))
            self.assertEqual(len(project_storage.list_egg_versions()), 0)

            self.assertIsNone(session.query(Project).filter_by(name=project_name).first())

            self.assertEqual(0, len(session.query(Spider).filter_by(project_id=project.id).all()))