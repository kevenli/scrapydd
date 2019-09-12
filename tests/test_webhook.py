import unittest
from scrapydd.webhook import *
from scrapydd.models import WebhookJob
from six import StringIO
import tornado
import tornado.web
from tornado.testing import AsyncTestCase, AsyncHTTPTestCase
import json


class WebhookRequestHandler(tornado.web.RequestHandler):
    def initialize(self, test):
        self.test = test

    def post(self):
        rows = []
        for key, values in self.request.body_arguments.items():
            if len(rows) == 0:
                rows = [{} for x in range(len(values))]
            if len(rows) != len(values):
                raise Exception('rows are not aligned')
            for i, value in enumerate(values):
                rows[i][key] = value
        self.test.batches.append(rows)

@unittest.skip
class WebhookJobExecutorTest(AsyncHTTPTestCase):
    def setUp(self):
        super(WebhookJobExecutorTest, self).setUp()
        self.batches = []
        os.environ['ASYNC_TEST_TIMEOUT'] = '30'

    def get_app(self):
        return tornado.web.Application([
            ('/update$', WebhookRequestHandler, {'test':self}),
        ])

    @tornado.testing.gen_test
    def test_execute(self):
        data = {'a':1}
        item_file = StringIO(json.dumps(data))
        job = WebhookJob()
        job.id=1
        job.items_file=item_file
        job.job_id=1

        http_port = self.get_http_port()
        job.payload_url='http://localhost:%s/update' % http_port

        target = WebhookJobExecutor(job, item_file, 1000)
        actual = yield target.start()
        self.assertEqual(job, actual)
        # only one post
        self.assertEqual(len(self.batches), 1)
        # the first post has one row
        self.assertEqual(len(self.batches[0]), 1)
        # the data of first post
        self.assertEqual(self.batches[0][0]['a'],'1')

    @unittest.skip
    @tornado.testing.gen_test
    def test_execute_over_memory_limit(self):
        item_file = StringIO('{"a":1}')
        job = WebhookJob()
        job.id=1
        job.items_file=item_file
        job.job_id=1

        http_port = self.get_http_port()
        job.payload_url='http://localhost:%s/update' % http_port

        target = WebhookJobExecutor(job, item_file, 1)
        try:
            job = yield target.start()
            self.fail('WebhookMemoryLimitError not catched')
        except WebhookJobOverMemoryLimitError as e:
            self.assertEqual(e.executor.job, job)
        # no data actually posted
        self.assertEqual(len(self.batches), 0)

    @unittest.skip
    @tornado.testing.gen_test
    def test_execute_jl_decoding_error(self):
        item_file = StringIO('{"a":11,12,3,123,}')
        job = WebhookJob()
        job.id=1
        job.items_file=item_file
        job.job_id=1

        http_port = self.get_http_port()
        job.payload_url='http://localhost:%s/update' % http_port

        target = WebhookJobExecutor(job, item_file, 10000)
        try:
            job = yield target.start()
            self.fail('WebhookJobJlDecodeError not catched')
        except WebhookJobJlDecodeError as e:
            self.assertEqual(job, e.executor.job)
            self.assertEqual(e.message, 'Error when decoding jl file')
        self.assertEqual(len(self.batches), 0)

    @unittest.skip
    @tornado.testing.gen_test
    def test_execute_webhook_address_not_reachable(self):
        item_file = StringIO('{"a":1}')
        job = WebhookJob()
        job.id=1
        job.items_file=item_file
        job.job_id=1

        job.payload_url='http://notreachable/update'

        target = WebhookJobExecutor(job, item_file, 10000)
        try:
            job = yield target.start()
            self.fail('WebhookJobJlDecodeError not catched')
        except Exception as e:
            #self.assertEqual(job, e.executor.job)
            logging.debug(e.message)
            self.assertIsNotNone(e.message)
            self.assertIsNotNone(str(e))
        self.assertEqual(len(self.batches), 0)

    @tornado.testing.gen_test
    def test_execute_batch_1(self):
        data = [{'a':1},
                {'a':2},
                {'a':3}]
        item_file = StringIO()
        batch_size = 1
        for row in data:
            item_file.write(json.dumps(row) + os.linesep)
        item_file.seek(0)

        job = WebhookJob()
        job.id = 1
        job.items_file=item_file
        job.job_id = 1

        http_port = self.get_http_port()
        job.payload_url = 'http://localhost:%s/update' % http_port

        target = WebhookJobExecutor(job, item_file, 10000, max_batch_size=batch_size)
        yield target.start()

        self.assertEqual(len(self.batches), 3)
        self.assertEqual(len(self.batches[0]), 1)
        self.assertEqual(self.batches[0][0]['a'], '1')
        self.assertEqual(len(self.batches[1]), 1)
        self.assertEqual(self.batches[1][0]['a'], '2')
        self.assertEqual(len(self.batches[1]), 1)
        self.assertEqual(self.batches[2][0]['a'], '3')





