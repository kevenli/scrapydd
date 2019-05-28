from tornado.testing import gen_test, AsyncTestCase
from tornado.ioloop import IOLoop
from scrapydd.workspace import ProjectWorkspace
from scrapydd.exceptions import ProcessFailed
import tempfile
import os

class ProjectWorkspaceTest(AsyncTestCase):
    @gen_test(timeout=30)
    def test_init(self):
        target = ProjectWorkspace('test_project')

        yield target.init()
        self.assertTrue(os.path.exists(target.python))
        self.assertTrue(os.path.exists(target.pip))

        self.assertTrue(file_is_in_dir(tempfile.gettempdir(), target.python))

    @gen_test(timeout=30)
    def test_init_kill(self):
        target = ProjectWorkspace('test_project')

        IOLoop.current().call_later(1, target.kill_process)

        try:
            yield target.init()
            self.fail('Exception not caught')
        except ProcessFailed:
            pass
        except Exception as e:
            self.fail('ProcessFailed exception not caught. %s' % e)
        self.assertEqual(len(target.processes), 0)

def file_is_in_dir(dir, file):
    if os.path.dirname(file) == dir:
        return True

    parent_dir = os.path.dirname(file)
    if parent_dir == file:
        return False

    return file_is_in_dir(dir, parent_dir)