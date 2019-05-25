import tornado.web
import logging
from scrapydd.webhook import WebhookJobExecutor
from scrapydd.models import WebhookJob

class DataReceiver(tornado.web.RequestHandler):
    def post(self):
        rows = []
        for key, values in self.request.body_arguments.items():
            if len(rows) == 0:
                rows = [{} for x in range(len(values))]
            if len(rows) != len(values):
                raise Exception('rows are not aligned')
            for i, value in enumerate(values):
                rows[i][key] = value

        print('%s rows received' % len(rows))
        #for row in rows:
        #    print row

def make_app():
    return tornado.web.Application([
        (r"/webhook", DataReceiver),
    ])

def main():
    logging.basicConfig(level=logging.DEBUG)
    # job = WebhookJob()
    # job.items_file = '../56764055f1464baba6e5749f4e00ef44.jl'
    # job.payload_url = 'http://localhost:6803/webhook'
    # executor = WebhookJobExecutor(job = job)
    # future = executor.start()
    app = make_app()
    app.listen(6803)
    print('http://localhost:%d/webhook' % 6803)
    ioloop = tornado.ioloop.IOLoop.current()
    # def job_done(future):
    #     logging.debug('job finished.')
    #     ioloop.stop()
    #
    # future.add_done_callback(job_done)
    ioloop.start()

if __name__ == '__main__':
    main()

