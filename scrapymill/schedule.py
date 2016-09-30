
from models import Session, Trigger, Spider, Project, SpiderExecutionQueue
from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.cron import CronTrigger
import sys,os
import subprocess
from tornado.process import Subprocess
from sqlite3 import IntegrityError

import logging
import datetime

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
            self.add_job(trigger.id, trigger.cron_pattern)
        self.scheduler.start()
        session.close()

    def add_job(self, trigger_id, cron):
        cron_parts = cron.split(' ')
        crontrigger = CronTrigger(minute=cron_parts[0],
                                  hour=cron_parts[1],
                                  day=cron_parts[2],
                                  month=cron_parts[3],
                                  day_of_week=cron_parts[4],
                                  )

        self.scheduler.add_job(func=self.trigger_fired, trigger=crontrigger, kwargs={'trigger_id': trigger_id},
                               id=str(trigger_id))

    def process_exit_callback(self):
        print 'process finished'

    def trigger_fired(self, trigger_id):
        session = Session()
        trigger = session.query(Trigger).filter_by(id=trigger_id).first()
        spider = session.query(Spider).filter_by(id=trigger.spider_id).first()
        project = session.query(Project).filter_by(id=spider.project_id).first()
        executing = session.query(SpiderExecutionQueue).filter_by(spider_id = spider.id).first()
        if executing:
            self.logger.warning('spider %s-%s is still running or in queue, skipping' % (project.name, spider.name))
            session.close()
            return

        executing = SpiderExecutionQueue()
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
        runner = 'scrapyd.runner'
        pargs = [sys.executable, '-m', runner, 'list']
        env = os.environ.copy()
        env['SCRAPY_PROJECT'] = str(project.name)
        subprocess.check_call(pargs, env=env)
        #subprocess = Subprocess(pargs, env=env)
        #subprocess.set_exit_callback(self.process_exit_callback)
        #subprocess.wait_for_exit()




        print 'trigger fired %s ' % trigger_id