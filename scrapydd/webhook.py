from tornado.ioloop import IOLoop, PeriodicCallback
import json
import urllib
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
import logging
from tornado.concurrent import Future
from models import Session, WebhookJob, SpiderWebhook, session_scope, SpiderSettings
import os, os.path, shutil
import sys
from .exceptions import *

logger = logging.getLogger(__name__)


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
        with session_scope() as session:
            # reset all running jobs to PENDING status on start
            for job in session.query(WebhookJob).filter(WebhookJob.status == 1):
                job.status = 0
                session.add(job)
            session.commit()

    def start_job(self, job_id):
        with session_scope() as session:
            job = session.query(WebhookJob).filter_by(id=job_id).first()
            job.status = 1
            session.add(job)
            session.commit()

    def finish_job(self, job_id):
        with session_scope() as session:
            job = session.query(WebhookJob).filter_by(id=job_id).first()
            job.status = 2
            session.add(job)
            session.commit()

    def job_failed(self, job_id):
        with session_scope() as session:
            job = session.query(WebhookJob).filter_by(id=job_id).first()
            job.status = 3
            session.add(job)
            session.commit()

    def get_next_job(self):
        with session_scope() as session:
            next_jobs = list(session.query(WebhookJob).filter(WebhookJob.status == 0).order_by(WebhookJob.id).slice(0, 1))
            if next_jobs:
                return next_jobs[0]

    def add_job(self, job_id, payload_url, items_file, spider_id):
        '''

        @type job_id : str
        @param job_id: The id of the spider job
        @type payload_url : str
        @param payload_url : The target webhook payload url.
        @type items_file : str
        @param items_file : the items jl file path
        :return:
        '''
        with session_scope() as session:
            job = WebhookJob()
            job.job_id = job_id
            job.payload_url = payload_url
            job.items_file = items_file
            job.spider_id = spider_id
            job.status = 0
            session.add(job)



class WebhookJobExecutor():
    def __init__(self, job, item_file, memory_limit, max_batch_size=0):
        self.payload_url = job.payload_url
        self.item_file = item_file
        self.future = Future()
        self.job = job
        self.ioloop = IOLoop.current()
        self.send_msg_interval = 0.1
        self.memory_limit = memory_limit
        self.max_batch_size = max_batch_size

    def start(self):
        self.ioloop.call_later(self.send_msg_interval, self.send_next_msg)
        return self.future

    def send_next_msg(self):
        row_count = 0
        rows = []

        max_batch_size = self.max_batch_size
        while True:
            line = self.item_file.readline()
            if line == '':
                break
            try:
                line_data = json.loads(line.decode('utf8'))
                row_count += 1
                rows.append(line_data)
            except ValueError as e:
                logger.error('Error when decoding jl file. %s', e.message)
                exc = WebhookJobJlDecodeError(executor=self, message='Error when decoding jl file')
                self.on_error(exc)
                return

            if sys.getsizeof(rows) > self.memory_limit:
                message = 'Webhook task memory usage over %s' % self.memory_limit
                logger.warning(message)
                exc = WebhookJobOverMemoryLimitError(executor=self, message=message)
                self.on_error(exc)
                return

            if max_batch_size > 0 and row_count >= max_batch_size:
                break

        if len(rows) > 0:
            keys = []
            post_dict = {}
            for row in rows:
                # encode unicode fields
                for key, value in row.items():
                    if isinstance(value, unicode):
                        row[key] = value.encode('utf8')

                # fill all keys
                keys = list(set(keys + row.keys()))

            for key in keys:
                post_dict[key] = [row.get(key, '') for row in rows ]

            post_data = urllib.urlencode(post_dict, doseq=True)

            client = AsyncHTTPClient()
            request = HTTPRequest(url=self.payload_url,
                                  method='POST',
                                  body=post_data)
            future = client.fetch(request)
            future.add_done_callback(self.schedule_next_send)
        # no data left in file, complete
        else:
            self.finish_job()

    def finish_job(self):
        if self.item_file:
            self.item_file.close()
        self.future.set_result(self.job)


    def on_error(self, exc):
        logging.error('Webhook job failed, %s' % exc)
        self.item_file.close()

        self.future.set_exception(exc)


    def schedule_next_send(self, callback_future=None):
        if callback_future is not None:
            exc = callback_future.exception()
            if exc is not None:
                self.on_error(WebhookJobFailedError(executor=self, message=str(exc), inner_exc=exc))
                return
        self.ioloop.call_later(self.send_msg_interval, self.send_next_msg)


class WebhookDaemon():
    queue_file_dir = 'webhook'

    def __init__(self, config, spider_setting_loader):
        logger.debug('webhookdaemon start')
        self.ioloop = IOLoop.current()
        self.poll_next_job_callback = PeriodicCallback(self.check_job, 10*1000)
        self.current_job = None
        self.storage = DatabaseWebhookJobStateStorage()
        self.poll_next_job_callback.start()
        self.webhook_memory_limit = config.getint('webhook_memory_limit')
        self.spider_setting_loader = spider_setting_loader

        if not os.path.exists(self.queue_file_dir):
            os.makedirs(self.queue_file_dir)

    def check_job(self):
        logger.debug('webhook daemon checkjob.')
        if self.current_job is None:
            next_job = self.storage.get_next_job()
            if next_job:
                items_file = open(next_job.items_file, 'r')

                max_batch_size = 0
                if next_job.spider_id:
                    max_batch_size_setting = self.spider_setting_loader.get_spider_setting(next_job.spider_id, 'webhook_batch_size')
                    if max_batch_size_setting:
                        try:
                            max_batch_size = int(max_batch_size_setting.value)
                        except ValueError:
                            # if setting is not configured correct, set it to 0 by default.
                            max_batch_size = 0

                self.current_job = WebhookJobExecutor(next_job, items_file, self.webhook_memory_limit, max_batch_size=max_batch_size)
                try:
                    future = self.current_job.start()
                    self.storage.start_job(next_job.id)
                    future.add_done_callback(self.job_finised)
                except Exception as e:
                    logger.error('Error when starting webhook job: %s', e.message)
                    self.job_failed(next_job)


    def init(self):
        self.poll_next_job_callback.start()

    def job_finised(self, future):
        try:
            job = future.result()
            self.storage.finish_job(job.id)
            self.current_job = None
            if os.path.exists(job.items_file) and os.path.dirname(job.items_file) == self.queue_file_dir:
                os.remove(job.items_file)
            logger.info('webhook job %s finished', job.id)
        # except WebhookJobOverMemoryLimitError as e:
        #     job = e.job
        #     self.job_failed(job)
        # except WebhookJobJlDecodeError as e:
        #     job = e.job
        #     self.job_failed(job)
        except WebhookJobFailedError as e:
            executor = e.executor
            job = executor.job
            self.job_failed(job)
            logger.error(e)

    def job_failed(self, job):
        self.storage.job_failed(job.id)
        self.current_job = None
        if os.path.exists(job.items_file) and os.path.dirname(job.items_file) == self.queue_file_dir:
            os.remove(job.items_file)
        logger.info('webhook job %s failed', job.id)

    def on_spider_complete(self, job, items_file):
        with session_scope() as session:
            webhook_setting = session.query(SpiderSettings).filter_by(spider_id = job.spider_id, setting_key='webhook_payload').first()
            if webhook_setting and webhook_setting.value:
                webhook_payload_url = webhook_setting.value
                task_items_file = os.path.join(self.queue_file_dir, os.path.basename(items_file))
                shutil.copy(items_file, task_items_file)
                self.storage.add_job(job.id, webhook_payload_url, task_items_file, job.spider_id)
