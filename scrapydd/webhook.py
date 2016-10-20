from tornado.ioloop import IOLoop, PeriodicCallback
import json
import urllib
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
import logging
from tornado.concurrent import Future
from models import Session, WebhookJob, SpiderWebhook


def encode_data(data):
    '''
    Encode dict to http postable binary array
    @type data: dict
    @param data: the data dict to encode
    :return:
    '''
    tmp = {}
    for key, value in data.items():
        if isinstance(value,(int, long, str)):
            tmp[key] = value
        elif isinstance(value, unicode):
            tmp[key] = value.encode('utf8')
        elif isinstance(value, (dict, tuple, list)):
            tmp[key] = json.dumps(value)
        else:
            raise ValueError(type(value))
    return urllib.urlencode(tmp)

class WebhookJobStateStorage():
    def start_job(self, job_id):
        raise NotImplementedError()

    def finish_job(self, job_id):
        raise NotImplementedError()

    def get_next_job(self):
        raise NotImplementedError()

    def add_job(self, job_id, payload_url, items_file):
        '''

        @type job_id : str
        @param job_id: The job append to jab queue
        :return:
        '''
        raise NotImplementedError()


class DatabaseWebhookJobStateStorage(WebhookJobStateStorage):
    def __init__(self):
        session = Session()
        # reset all running jobs to PENDING status on start
        for job in session.query(WebhookJob).filter(WebhookJob.status == 1):
            job.status = 0
            session.add(job)
        session.commit()
        session.close()

    def start_job(self, job_id):
        session = Session()
        job = session.query(WebhookJob).filter_by(id=job_id).first()
        job.status = 1
        session.add(job)
        session.commit()
        session.close()

    def finish_job(self, job_id):
        session = Session()
        job = session.query(WebhookJob).filter_by(id=job_id).first()
        job.status = 2
        session.add(job)
        session.commit()
        session.close()

    def get_next_job(self):
        session = Session()
        next_jobs = list(session.query(WebhookJob).filter(WebhookJob.status == 0).order_by(WebhookJob.id).slice(0, 1))
        if next_jobs:
            return next_jobs[0]
        session.close()

    def add_job(self, job_id, payload_url, items_file):
        '''

        @type job_id : str
        @param job_id: The id of the spider job
        @type payload_url : str
        @param payload_url : The target webhook payload url.
        @type items_file : str
        @param items_file : the items jl file path
        :return:
        '''
        job = WebhookJob()
        job.job_id = job_id
        job.payload_url = payload_url
        job.items_file = items_file
        job.status = 0
        session = Session()
        session.add(job)
        session.commit()
        session.refresh(job)
        session.close()


class WebhookJobExecutor():
    def __init__(self, job):
        self.payload_url = job.payload_url
        self.item_file = open(job.items_file, 'r')
        self.future = Future()
        self.job = job
        self.ioloop = IOLoop.current()

    def start(self):
        self.ioloop.call_later(1, self.send_next_msg)
        return self.future

    def send_next_msg(self, callback_future=None):
        line = self.item_file.readline()
        # if read new line, keep going
        if line:
            logging.debug(len(line))
            try:
                line_data = json.loads(line.decode('utf8'))
                client = AsyncHTTPClient()
                request = HTTPRequest(url=self.payload_url,
                                      method='POST',
                                      body=encode_data(line_data))
                future = client.fetch(request)
                future.add_done_callback(self.schedule_next_send)
            except ValueError as e:
                logging.error('Error when docoding jl file.' + e.message)
                self.ioloop.call_later(1, self.schedule_next_send)
        # no data left in file, complete
        else:
            self.finish_job()


    def finish_job(self):
        if self.item_file:
            self.item_file.close()
        self.future.set_result(self.job)

    def schedule_next_send(self, future=None):
        self.ioloop.call_later(1, self.send_next_msg)


class WebhookDaemon():
    def __init__(self):
        logging.debug('webhookdaemon start')
        self.ioloop = IOLoop.current()
        self.poll_next_job_callback = PeriodicCallback(self.check_job, 10*1000)
        self.current_job = None
        self.storage = DatabaseWebhookJobStateStorage()
        self.poll_next_job_callback.start()

    def check_job(self):
        logging.debug('webhook daemon checkjob.')
        if self.current_job is None:
            next_job = self.storage.get_next_job()
            if next_job:
                self.current_job = WebhookJobExecutor(next_job)
                future = self.current_job.start()
                self.storage.start_job(next_job.id)
                future.add_done_callback(self.job_finised)


    def init(self):
        self.poll_next_job_callback.start()

    def job_finised(self, future):
        job = future.result()
        self.storage.finish_job(job.id)
        self.current_job = None
        logging.info('webhook job %s finished', job.id)

    def on_spider_complete(self, job, items_file):
        session = Session()
        webhook = session.query(SpiderWebhook).filter_by(id = job.spider_id).first()
        if webhook:
            self.storage.add_job(job.id, webhook.payload_url, items_file)
        session.close()
