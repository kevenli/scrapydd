
from models import Session, Trigger, Spider, Project, SpiderExecutionQueue
from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.cron import CronTrigger
from sqlite3 import IntegrityError
import uuid
import logging
import datetime
from .exceptions import *

def generate_jobid():
    jobid = str(uuid.uuid4()).replace('-', '')
    return jobid


class SchedulerManager:
    def __init__(self):
        executors = {
            'default': ThreadPoolExecutor(20),
            'processpool': ProcessPoolExecutor(5)
        }
        self.scheduler = TornadoScheduler(executors=executors)

    def init(self):
        self.logger = logging.getLogger(__name__)
        session = Session()
        triggers = session.query(Trigger)
        for trigger in triggers:
            try:
                self.add_job(trigger.id, trigger.cron_pattern)
            except InvalidCronExpression:
                logging.warning('Trigger %d,%s cannot be added ' % (trigger.id, trigger.cron_pattern))
        self.scheduler.start()
        session.close()

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

        self.scheduler.add_job(func=self.trigger_fired, trigger=crontrigger, kwargs={'trigger_id': trigger_id},
                               id=str(trigger_id))

    def process_exit_callback(self):
        print 'process finished'


    def trigger_fired(self, trigger_id):
        session = Session()
        trigger = session.query(Trigger).filter_by(id=trigger_id).first()
        spider = session.query(Spider).filter_by(id=trigger.spider_id).first()
        project = session.query(Project).filter_by(id=spider.project_id).first()
        executing = session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.spider_id == spider.id, SpiderExecutionQueue.status.in_([0,1])).first()
        if executing:
            self.logger.warning('spider %s-%s is still running or in queue, skipping' % (project.name, spider.name))
            session.close()
            return

        executing = SpiderExecutionQueue()
        executing.id = generate_jobid()
        executing.spider_id = spider.id
        executing.project_name = project.name
        executing.spider_name = spider.name
        executing.fire_time = datetime.datetime.now()
        executing.update_time = datetime.datetime.now()
        session.add(executing)
        try:
            session.commit()
        except (Exception, IntegrityError) as e:
            self.logger.warning(e)
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
                logging.warning('job %s_%s is running, ignoring schedule'%(project.name, spider.name))
                raise JobRunning(existing[0].id)
            executing = SpiderExecutionQueue()
            jobid = generate_jobid()
            executing.id = jobid
            executing.spider_id = spider.id
            executing.project_name = project.name
            executing.spider_name = spider.name
            executing.fire_time = datetime.datetime.now()
            executing.update_time = datetime.datetime.now()
            session.add(executing)
        finally:
            session.commit()
            session.close()

        return jobid


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
        finished = list(session.query(SpiderExecutionQueue).filter(SpiderExecutionQueue.status==2).slice(0, 100))
        session.close()
        return pending, running, finished

    def job_start(self, jobid, pid):
        session = Session()
        job = session.query(SpiderExecutionQueue).filter_by(id=jobid).first()
        job.update_time = datetime.datetime.now()
        job.pid = pid
        session.add(job)
        session.commit()
        session.close()
