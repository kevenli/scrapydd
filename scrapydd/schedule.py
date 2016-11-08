
from models import Session, Trigger, Spider, Project, SpiderExecutionQueue, HistoricalJob, session_scope
from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.cron import CronTrigger
from sqlite3 import IntegrityError
from tornado.ioloop import IOLoop, PeriodicCallback
import uuid
import logging
import datetime
from .exceptions import *
from Queue import Queue, Empty
from config import Config
from sqlalchemy import distinct, desc
import os
from pysyncobj import SyncObj, replicated


def generate_job_id():
    jobid = uuid.uuid4().hex
    return jobid

logger = logging.getLogger(__name__)


class SchedulerManager():
    def __init__(self, task_id, config=None):
        if config is None:
            config = Config()
        self.config = config
        self.task_id = task_id
        executors = {
            'default': ThreadPoolExecutor(20),
            'processpool': ProcessPoolExecutor(5)
        }
        self.scheduler = TornadoScheduler(executors=executors)
        self.task_queue = Queue()
        self.poll_task_queue_callback = None
        self.pool_task_queue_interval = 10
        self.ioloop = IOLoop.instance()
        self.poll_task_queue_callback = PeriodicCallback(self.poll_task_queue, self.pool_task_queue_interval * 1000)
        self.clear_finished_jobs_callback = PeriodicCallback(self.clear_finished_jobs, 60*1000)
        self.reset_timeout_job_callback = PeriodicCallback(self.reset_timeout_job, 60*1000)

        if task_id:
            sync_address = config.get('cluster_bind_address')
            sync_ports = config.get('cluster_bind_ports').split(',')
            sync_port = int(sync_ports[task_id])
            peers = config.get('cluster_peers').split(',') if config.get('cluster_peers') else []
            peers = peers + ['%s:%d' % (sync_address,int(port)) for port in sync_ports if int(port) != sync_port]
            print '%s:%d' % (sync_address, sync_port)
            print peers
            try:
                self.sync_obj = ScheduleSyncObj(sync_address, sync_port, peers)
            except Exception as e:
                print type(e)
        else:
            self.sync_obj = NoSync()
        self.sync_obj.set_on_remove_schedule_job(self.remove_scheduling_job)
        self.sync_obj.set_on_add_schedule_job(self.add_scheduling_job)
        #else:
        #    print 'error'

    def init(self):
        session = Session()

        # move completed jobs into history
        for job in session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.status.in_((2, 3))):
            historical_job = HistoricalJob()
            historical_job.id = job.id
            historical_job.spider_id = job.spider_id
            historical_job.project_name = job.project_name
            historical_job.spider_name = job.spider_name
            historical_job.fire_time = job.fire_time
            historical_job.start_time = job.start_time
            historical_job.complete_time = job.update_time
            historical_job.status = job.status
            session.delete(job)
            session.add(historical_job)
        session.commit()

        # init triggers
        triggers = session.query(Trigger)
        for trigger in triggers:
            try:
                self.add_job(trigger.id, trigger.cron_pattern)
            except InvalidCronExpression:
                logger.warning('Trigger %d,%s cannot be added ' % (trigger.id, trigger.cron_pattern))
        session.close()

        self.scheduler.start()

        self.poll_task_queue_callback.start()
        self.clear_finished_jobs_callback.start()
        self.reset_timeout_job_callback.start()

    def poll_task_queue(self):
        if self.task_queue.empty():
            with session_scope() as session:
                tasks_to_run = session.query(SpiderExecutionQueue).filter_by(status=0).order_by(
                    SpiderExecutionQueue.update_time).slice(0, 10)
                for task in tasks_to_run:
                    self.task_queue.put(task)

    def add_job(self, trigger_id, cron):
        cron_parts = cron.split(' ')
        if len(cron_parts) != 5:
            raise InvalidCronExpression()

        try:
            crontrigger = CronTrigger(minute=cron_parts[0],
                                      hour=cron_parts[1],
                                      day=cron_parts[2],
                                      month=cron_parts[3],
                                      day_of_week=cron_parts[4],
                                      )
        except ValueError:
            raise InvalidCronExpression()

        #job = self.scheduler.add_job(func=self.trigger_fired, trigger=crontrigger, kwargs={'trigger_id': trigger_id},
        #                       id=str(trigger_id), replace_existing=True)
        self.sync_obj.add_schedule_job(trigger_id)

    def remove_scheduling_job(self, job_id):
        self.scheduler.remove_job(job_id)

    def add_scheduling_job(self, trigger_id):
        with session_scope() as session:
            trigger = session.query(Trigger).filter_by(id = trigger_id).first()
            cron = trigger.cron_pattern
            cron_parts = cron.split(' ')
            if len(cron_parts) != 5:
                raise InvalidCronExpression()

            try:
                crontrigger = CronTrigger(minute=cron_parts[0],
                                          hour=cron_parts[1],
                                          day=cron_parts[2],
                                          month=cron_parts[3],
                                          day_of_week=cron_parts[4],
                                          )
            except ValueError:
                raise InvalidCronExpression()

            job = self.scheduler.add_job(func=self.trigger_fired, trigger=crontrigger,
                                         kwargs={'trigger_id': trigger_id},
                                         id=str(trigger_id), replace_existing=True)

    def trigger_fired(self, trigger_id):
        session = Session()
        trigger = session.query(Trigger).filter_by(id=trigger_id).first()
        spider = session.query(Spider).filter_by(id=trigger.spider_id).first()
        project = session.query(Project).filter_by(id=spider.project_id).first()
        executing = session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.spider_id == spider.id, SpiderExecutionQueue.status.in_([0,1])).first()
        if executing:
            logger.warning('spider %s-%s is still running or in queue, skipping' % (project.name, spider.name))
            session.close()
            return

        executing = SpiderExecutionQueue()
        executing.id = generate_job_id()
        executing.spider_id = spider.id
        executing.project_name = project.name
        executing.spider_name = spider.name
        executing.fire_time = datetime.datetime.now()
        executing.update_time = datetime.datetime.now()
        session.add(executing)
        try:
            session.commit()
        except (Exception, IntegrityError) as e:
            logger.warning(e)
        session.close()
        return

    def add_schedule(self, project, spider, cron):
        session = Session()
        project = session.query(Project).filter(Project.name == project).first()
        if project is None:
            raise ProjectNotFound()

        spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider).first()
        if spider is None:
            raise SpiderNotFound()

        triggers = session.query(Trigger).filter(Trigger.spider_id==spider.id)
        found = False
        for trigger in triggers:
            if trigger.cron_pattern == cron:
                found = True
                break

        if not found:
            trigger = Trigger()
            trigger.spider_id = spider.id
            trigger.cron_pattern = cron
            session.add(trigger)
            session.commit()
            self.add_job(trigger.id, cron)
        session.close()

    def add_task(self, project_name, spider_name):
        session = Session()
        project = session.query(Project).filter(Project.name==project_name).first()
        spider = session.query(Spider).filter(Spider.name==spider_name, Spider.project_id==project.id).first()

        try:
            existing = list(session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.spider_id==spider.id, SpiderExecutionQueue.status.in_([0,1])))
            if existing:
                logger.warning('job %s_%s is running, ignoring schedule'%(project.name, spider.name))
                raise JobRunning(existing[0].id)
            executing = SpiderExecutionQueue()
            jobid = generate_job_id()
            executing.id = jobid
            executing.spider_id = spider.id
            executing.project_name = project.name
            executing.spider_name = spider.name
            executing.fire_time = datetime.datetime.now()
            executing.update_time = datetime.datetime.now()
            session.add(executing)
            session.commit()
            session.refresh(executing)
            return executing
        finally:
            session.close()

    def on_node_expired(self, node_id):
        session = Session()
        for job in session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.node_id==node_id, SpiderExecutionQueue.status == 1):
            job.status = 0
            job.update_time = datetime.datetime.now()
            session.add(job)
        session.commit()
        session.close()

    def jobs(self):
        session = Session()
        pending = list(session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.status==0))
        running = list(session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.status==1))
        finished = list(session.query(HistoricalJob).slice(0, 100))
        session.close()
        return pending, running, finished

    def job_start(self, jobid, pid):
        session = Session()
        job = session.query(SpiderExecutionQueue).filter_by(id=jobid).first()
        job.start_time = datetime.datetime.now()
        job.update_time = datetime.datetime.now()
        job.pid = pid
        session.add(job)
        session.commit()
        session.close()

    def get_next_task(self, node_id):
        if not self.task_queue.empty():
            session = Session()
            try:
                next_task = self.task_queue.get_nowait()
            except Empty:
                return None
            next_task = session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.id == next_task.id,
                                                                   SpiderExecutionQueue.status == 0).first()
            if not next_task:
                return None
            next_task.start_time = datetime.datetime.now()
            next_task.update_time = datetime.datetime.now()
            next_task.node_id = node_id
            next_task.status = 1
            session.add(next_task)
            session.commit()
            session.refresh(next_task)
            session.close()
            return next_task
        return None

    def has_task(self):
        return not self.task_queue.empty()

    def jobs_running(self, node_id, job_ids):
        session = Session()
        for job_id in job_ids:
            job = session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.id == job_id, SpiderExecutionQueue.status==1).first()
            if job:
                job.update_time = datetime.datetime.now()
                session.add(job)
        session.commit()
        session.close()

    def job_finished(self, job, log_file=None, items_file=None):
        session = Session()
        if job.status not in (2,3):
            raise Exception('Invliad status.')
        job_status = job.status
        job = session.query(SpiderExecutionQueue).filter_by(id=job.id).first()
        job.status = job_status
        job.update_time = datetime.datetime.now()

        historical_job = HistoricalJob()
        historical_job.id = job.id
        historical_job.spider_id = job.spider_id
        historical_job.project_name = job.project_name
        historical_job.spider_name = job.spider_name
        historical_job.fire_time = job.fire_time
        historical_job.start_time = job.start_time
        historical_job.complete_time = job.update_time
        historical_job.status = job.status
        if log_file:
            historical_job.log_file = log_file
            import re
            items_crawled_pattern = re.compile("\'item_scraped_count\': (\d+),")
            with open(log_file, 'r') as f:
                log_content = f.read()
                m = items_crawled_pattern.search(log_content)
                logger.debug(m)
                if m:
                    historical_job.items_count = int(m.group(1))

        if items_file:
            historical_job.items_file = items_file
        session.delete(job)
        session.add(historical_job)
        session.commit()
        session.refresh(historical_job)
        session.close()
        return historical_job

    def clear_finished_jobs(self):
        job_history_limit_each_spider = 100
        with session_scope() as session:
            spiders = list(session.query(distinct(HistoricalJob.spider_id)))
        for row in spiders:
            spider_id = row[0]
            with session_scope() as session:
                over_limitation_jobs = list(session.query(HistoricalJob)\
                    .filter(HistoricalJob.spider_id==spider_id)\
                    .order_by(desc(HistoricalJob.complete_time))\
                    .slice(job_history_limit_each_spider, 1000)\
                    .all())
            for over_limitation_job in over_limitation_jobs:
                self._remove_histical_job(over_limitation_job)

    def _clear_running_jobs(self):
        with session_scope() as session:
            jobs = list(session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.status.in_([0,1])))
        for job in jobs:
            self._remove_histical_job(job)

    def reset_timeout_job(self):
        with session_scope() as session:
            timeout_time = datetime.datetime.now() - datetime.timedelta(minutes=1)
            for job in session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.update_time < timeout_time,
                                                                  SpiderExecutionQueue.status == 1):
                job.status = 0
                job.pid = None
                job.node_id = None
                job.update_time = datetime.datetime.now()
                session.add(job)
                logging.info('Job %s is timeout, reseting.' % job.id)
            session.commit()

    def _remove_histical_job(self, job):
        '''
        @type job: HistoricalJob
        '''

        with session_scope() as session:
            job = session.query(HistoricalJob).filter(HistoricalJob.id == job.id).first()
            if job.items_file:
                try:
                    os.remove(job.items_file)
                except Exception as e:
                    logger.warning(e.message)

            if job.log_file:
                try:
                    os.remove(job.log_file)
                except Exception as e:
                    logger.warning(e.message)

            original_log_file = os.path.join('logs', job.project_name, job.spider_name, '%s.log' % job.id)
            if os.path.exists(original_log_file):
                os.remove(original_log_file)

            original_items_file = os.path.join('items', job.project_name, job.spider_name, '%s.jl' % job.id)
            if os.path.exists(original_items_file):
                os.remove(original_items_file)
            session.delete(job)

    def remove_schedule(self, project_name, spider_name, trigger_id):
        with session_scope() as session:
            project = session.query(Project).filter(Project.name == project_name).first()
            spider = session.query(Spider).filter(Spider.project_id == project.id, Spider.name == spider_name).first()
            trigger = session.query(Trigger).filter(Trigger.spider_id==spider.id, Trigger.id == trigger_id).first()
            #self.scheduler.remove_job(str(trigger.id))
            #self._remove_schedule_job()
            self.sync_obj.remove_schedule_job(trigger.id)
            session.delete(trigger)


    def _add_schedule_job(self):
        print '_add_schedule_job'

    def _remove_schedule_job(self):
        print '_remove_schedule_job'

class ScheduleSyncObj(SyncObj):
    def __init__(self, bind_address, bind_port, peers):
        super(ScheduleSyncObj, self).__init__('%s:%d' % (bind_address, bind_port), peers)
        self.__counter = 0

    @replicated
    def add_schedule_job(self, trigger_id):
        logger.info('_add_schedule_job')
        if self.on_add_schedule_job is not None:
            self.on_add_schedule_job(trigger_id)

    def set_on_add_schedule_job(self, callback):
        self.on_add_schedule_job = callback

    @replicated
    def remove_schedule_job(self, trigger_id):
        logger.info('_remove_schedule_job %s' % trigger_id)
        if self.on_remove_schedule_job is not None:
            self.on_remove_schedule_job(str(trigger_id))

    def set_on_remove_schedule_job(self, callback):
        self.on_remove_schedule_job = callback

class NoSync():
    def add_schedule_job(self, trigger_id):
        logger.info('_add_schedule_job')
        if self.on_add_schedule_job is not None:
            self.on_add_schedule_job(trigger_id)

    def set_on_add_schedule_job(self, callback):
        self.on_add_schedule_job = callback

    def remove_schedule_job(self, trigger_id):
        if self.on_remove_schedule_job is not None:
            self.on_remove_schedule_job(str(trigger_id))

    def set_on_remove_schedule_job(self, callback):
        self.on_remove_schedule_job = callback