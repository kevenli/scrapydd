
from models import Session, Trigger
from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.cron import CronTrigger

class SchedulerManager:
    def __init__(self):
        executors = {
            'default': ThreadPoolExecutor(20),
            'processpool': ProcessPoolExecutor(5)
        }
        self.scheduler = TornadoScheduler(executors=executors)

    def init(self):
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

    def trigger_fired(self, trigger_id):
        print 'trigger fired %s ' % trigger_id