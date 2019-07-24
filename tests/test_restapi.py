from tornado.testing import AsyncHTTPTestCase
import json
from scrapydd.main import *

class AppTest(AsyncHTTPTestCase):
    @classmethod
    def setUpClass(cls):
        if os._exists('test.db'):
            os.remove('test.db')
        config = Config(values = {'database_url': 'sqlite:///test.db'})
        init_database(config)
        os.environ['ASYNC_TEST_TIMEOUT'] = '120'

    def get_app(self):
        config = Config()
        scheduler_manager = SchedulerManager(config=config)
        scheduler_manager.init()
        node_manager = NodeManager(scheduler_manager)
        node_manager.init()
        return make_app(scheduler_manager, node_manager, None)


class NodesHandlerTest(AppTest):
    def test_register(self):
        response = self.fetch('/api/v1/nodes', method="POST", body="")
        self.assertEqual(200, response.code)

        response_data = json.loads(response.body)
        new_node_id = response_data['id']

        with session_scope() as session:
            new_node = session.query(Node).filter_by(id=new_node_id).first()

        self.assertEqual('127.0.0.1', new_node.client_ip)
        self.assertEqual(datetime.date.today(), new_node.create_time.date())
        self.assertEqual(datetime.date.today(), new_node.last_heartbeat.date())
        self.assertEqual(True, new_node.isalive)
        self.assertEqual(None, new_node.tags)