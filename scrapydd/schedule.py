"""
Schedule job status and periodical jobs.
"""
import uuid
import logging
import datetime
import json
from abc import ABC
from typing import List
from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger
from tornado.ioloop import IOLoop, PeriodicCallback
from sqlalchemy import distinct, desc, func
from six import string_types, ensure_str
import chardet
from .models import Session, Trigger, Spider, Project, SpiderExecutionQueue
from .models import HistoricalJob, session_scope, SpiderSettings, Node
from .exceptions import NodeNotFound, InvalidCronExpression, JobRunning
from .config import Config
from .mail import MailSender
from .storage import ProjectStorage


LOGGER = logging.getLogger(__name__)


JOB_STATUS_PENDING = 0
JOB_STATUS_RUNNING = 1
JOB_STATUS_SUCCESS = 2
JOB_STATUS_FAIL = 3
JOB_STATUS_WARNING = 4
JOB_STATUS_STOPPING = 5
JOB_STATUS_CANCEL = 6


class JobNotFound(Exception):
    pass


class InvalidJobStatus(Exception):
    pass


def generate_job_id():
    """
    Generate unique job id.
    :return:
    """
    jobid = uuid.uuid4().hex
    return jobid


class JobObserver(ABC):
    def on_job_finished(self, job: HistoricalJob):
        pass


class SchedulerManager():
    _job_observers: List[JobObserver] = []

    def __init__(self, config=None, syncobj=None, scheduler=None):
        if config is None:
            config = Config()
        self.config = config

        self.project_storage_dir = config.get('project_storage_dir')
        if scheduler:
            self.scheduler = scheduler
        else:
            executors = {
                'default': ThreadPoolExecutor(20),
            }
            self.scheduler = TornadoScheduler(executors=executors)

        self.poll_task_queue_callback = None
        self.pool_task_queue_interval = 10
        self.ioloop = IOLoop.instance()
        self.clear_finished_jobs_callback = PeriodicCallback(self.clear_finished_jobs, 60*1000)
        self.reset_timeout_job_callback = PeriodicCallback(self.reset_timeout_job, 10*1000)

        self.sync_obj = syncobj
        if syncobj is not None:
            self.sync_obj.set_on_remove_schedule_job(self.on_cluster_remove_scheduling_job)
            self.sync_obj.set_on_add_schedule_job(self.on_cluster_add_scheduling_job)

    def init(self):
        session = Session()
        self._transfer_complete_jobs(session)
        # init triggers
        triggers = session.query(Trigger)
        for trigger in triggers:
            try:
                self.add_job(trigger.id, trigger.cron_pattern)
            except InvalidCronExpression:
                LOGGER.warning('Trigger %d,%s cannot be added ',
                               (trigger.id, trigger.cron_pattern))
        session.close()
        self.clear_finished_jobs_callback.start()
        self.reset_timeout_job_callback.start()

    def _transfer_complete_jobs(self, session):
        # move completed jobs into history
        for job in session.query(SpiderExecutionQueue)\
                .filter(SpiderExecutionQueue.status.in_((2, 3))):
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

    def build_cron_trigger(self, cron):
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
            return crontrigger
        except ValueError:
            raise InvalidCronExpression()

    def add_job(self, trigger_id, cron):
        LOGGER.debug('adding trigger %s %s' % (trigger_id, cron))
        crontrigger = self.build_cron_trigger(cron)

        job = self.scheduler.add_job(func=self.trigger_fired,
                                     trigger=crontrigger,
                                     kwargs={'trigger_id': trigger_id},
                                     id=str(trigger_id),
                                     replace_existing=True)
        if self.sync_obj:
            self.ioloop.call_later(0, self.sync_obj.add_schedule_job, trigger_id)

    def on_cluster_remove_scheduling_job(self, job_id):
        LOGGER.debug('on_cluster_remove_scheduling_job')
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)

    def on_cluster_add_scheduling_job(self, trigger_id):
        LOGGER.debug('on_cluster_add_scheduling_job')
        with session_scope() as session:
            trigger = session.query(Trigger).get(trigger_id)
            if trigger is None:
                return
            crontrigger = self.build_cron_trigger(trigger.cron_pattern)
            job = self.scheduler.add_job(func=self.trigger_fired, trigger=crontrigger,
                                         kwargs={'trigger_id': trigger_id},
                                         id=str(trigger_id), replace_existing=True)

    def trigger_fired(self, trigger_id):
        with session_scope() as session:
            trigger = session.query(Trigger).filter_by(id=trigger_id).first()
            if not trigger:
                LOGGER.error('Trigger %s not found.' % trigger_id)
                return

            spider = session.query(Spider).filter_by(id=trigger.spider_id).first()
            if not spider:
                LOGGER.error('Spider %s not found' % spider.name)
                return

            project = session.query(Project).filter_by(id=spider.project_id).first()
            if not project:
                LOGGER.error('Project %s not found' % project.name)
                return

            try:
                self.add_spider_task(session, spider)
            except JobRunning:
                LOGGER.info('Job for spider %s.%s already reach the '
                            'concurrency limit' % (project.name, spider.name))

    def add_schedule(self, project, spider, cron):
        with session_scope()as session:
            triggers = session.query(Trigger)\
                .filter(Trigger.spider_id == spider.id)
            found = False
            for trigger in triggers:
                if trigger.cron_pattern == cron:
                    found = True
                    break

            if not found:
                # create a cron_trigger for just validating
                cron_trigger = self.build_cron_trigger(cron)
                trigger = Trigger()
                trigger.spider_id = spider.id
                trigger.cron_pattern = cron
                session.add(trigger)
                session.commit()
                self.add_job(trigger.id, cron)

    def add_spider_task(self, session, spider, settings=None):
        executing = SpiderExecutionQueue()
        spider_tag_vo = session.query(SpiderSettings)\
            .filter_by(spider_id=spider.id, setting_key='tag').first()
        spider_tag = spider_tag_vo.value if spider_tag_vo else None
        jobid = generate_job_id()
        executing.id = jobid
        executing.spider_id = spider.id
        executing.project_name = spider.project.name
        executing.spider_name = spider.name
        executing.fire_time = datetime.datetime.now()
        executing.update_time = datetime.datetime.now()
        executing.tag = spider_tag
        if settings:
            executing.settings = json.dumps(settings)
        session.add(executing)
        session.commit()
        session.refresh(executing)
        return executing

    def add_task(self, project_name, spider_name, settings=None):
        with session_scope() as session:
            project = session.query(Project)\
                .filter(Project.name == project_name).first()
            spider = session.query(Spider)\
                .filter(Spider.name == spider_name,
                        Spider.project_id == project.id).first()
            executing = SpiderExecutionQueue()
            spider_tag_vo = session.query(SpiderSettings)\
                .filter_by(spider_id=spider.id, setting_key='tag').first()
            spider_tag = spider_tag_vo.value if spider_tag_vo else None
            jobid = generate_job_id()
            executing.id = jobid
            executing.spider_id = spider.id
            executing.project_name = project.name
            executing.spider_name = spider.name
            executing.fire_time = datetime.datetime.now()
            executing.update_time = datetime.datetime.now()
            executing.tag = spider_tag
            if settings:
                executing.settings = json.dumps(settings)
            session.add(executing)
            session.commit()
            session.refresh(executing)
            return executing

    def cancel_task(self, job_id):
        with session_scope() as session:
            job = session.query(SpiderExecutionQueue).get(job_id)
            if not job:
                raise JobNotFound()
            if job.status not in (JOB_STATUS_PENDING,
                                  JOB_STATUS_RUNNING):
                raise InvalidJobStatus('Invliad status.')
            job.status = JOB_STATUS_CANCEL
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
            session.delete(job)
            session.add(historical_job)

            session.commit()
            session.refresh(historical_job)

    def on_node_expired(self, node_id):
        session = Session()
        for job in session.query(SpiderExecutionQueue)\
                .filter(SpiderExecutionQueue.node_id == node_id,
                        SpiderExecutionQueue.status == 1):
            job.status = 0
            job.update_time = datetime.datetime.now()
            job.start_time = None
            job.pid = None
            job.node_id = None
            session.add(job)
        session.commit()
        session.close()

    def jobs(self, session):
        pending = list(session.query(SpiderExecutionQueue)
                       .filter_by(status=JOB_STATUS_PENDING))
        running = list(session.query(SpiderExecutionQueue)
                       .filter_by(status=JOB_STATUS_RUNNING))
        finished = list(session.query(HistoricalJob)
                        .order_by(desc(HistoricalJob.complete_time))
                        .slice(0, 100))
        return pending, running, finished

    def job_start(self, jobid, pid):
        with session_scope() as session:
            job = session.query(SpiderExecutionQueue).filter_by(id=jobid).first()
            if job.start_time is None:
                job.start_time = datetime.datetime.now()
            job.update_time = datetime.datetime.now()
            if job.pid is None and pid:
                job.pid = pid
            session.add(job)
            session.commit()
            session.close()

    def _regular_agent_tags(self, agent_tags):
        if agent_tags is None:
            return None
        if isinstance(agent_tags, string_types):
            return agent_tags.split(',')
        return agent_tags

    def get_next_task(self, node_id):
        """
        Get next task for node_id, if exists, update
        the job status, track it with node_id.
        :param node_id:
            node_id
        :return:
            the running job
        """
        with session_scope() as session:
            node = session.query(Node).filter(Node.id == node_id).first()
            if not node:
                raise NodeNotFound()

            node_tags = node.tags
            next_task = self._get_next_task(session, node_tags)
            if not next_task:
                return None
            now = self._now()
            next_task.start_time = now
            next_task.update_time = now
            next_task.node_id = node_id
            next_task.status = JOB_STATUS_RUNNING
            session.add(next_task)
            session.commit()
            session.refresh(next_task)
            return next_task

    def _get_next_task(self, session, agent_tags):
        # result = session.query(func.)
        #     obj, func.avg(obj.value).label("value_avg")
        # ).group_by(
        #     func.strftime('%s', obj.date)
        # ).all()
        result = session.execute("""
        select * from spider_execution_queue
join (select min(fire_time) as fire_time, spider_id
    from spider_execution_queue
where status=0
    group by spider_id
    ) as a
   on spider_execution_queue.fire_time = a.fire_time
    and spider_execution_queue.spider_id = a.spider_id
order by a.fire_time
""")
        for job in session.query(SpiderExecutionQueue).instances(result):
            spider_max_concurrency = 1
            spider_concurrency = session.query(
                func.count(SpiderExecutionQueue.id)
            )\
            .filter(SpiderExecutionQueue.status == JOB_STATUS_RUNNING) \
            .scalar() or 0
            if spider_concurrency >= spider_max_concurrency:
                continue

            spider_tags = self.get_spider_tags(job.spider, session)
            if self._match_tags(spider_tags, agent_tags):
                return job
        return None

    def _match_tags(self, spider_tags, node_tags):
        # both empty
        if not spider_tags and not node_tags:
            return True

        # one empty and one not
        if not spider_tags or not node_tags:
            return False

        for spider_tag in spider_tags:
            if spider_tag not in node_tags:
                return False
        return True

    def get_spider_tags(self, spider, session):
        tags_setting = session.query(SpiderSettings) \
            .filter(SpiderSettings.setting_key == 'tag',
                    SpiderSettings.spider_id == spider.id).first()
        if not tags_setting:
            return []

        if not tags_setting.value:
            return []

        return [x for x in tags_setting.value.split(',') if x]

    def has_task(self, node_id):
        with session_scope() as session:
            node = session.query(Node).filter(Node.id == node_id).first()
            if node is None:
                raise NodeNotFound()

            node_tags = self._regular_agent_tags(node.tags)
            next_task = self._get_next_task(session, node_tags)
        return next_task is not None


    def jobs_running(self, node_id, job_ids):
        '''
        Update running jobs for node.
        If any job status is wrong, let node kill it
        :param node_id:
        :param job_ids:
        :return:(job_id) to kill
        '''
        jobs_to_kill = []
        with session_scope() as session:
            for job_id in job_ids:
                job = session.query(SpiderExecutionQueue).filter(
                    SpiderExecutionQueue.id == job_id).first()

                if job:
                    if job.node_id is None:
                        job.node_id = node_id
                    if job.node_id != node_id or \
                        job.status != 1:
                        jobs_to_kill.append(job.id)
                    else:
                        job.update_time = self._now()
                        session.add(job)
                else:
                    jobs_to_kill.append(job_id)
            session.commit()
        return jobs_to_kill

    def job_finished(self, job, log_file=None, items_file=None):
        session = Session()
        if job.status not in (JOB_STATUS_SUCCESS, JOB_STATUS_FAIL):
            raise Exception('Invalid status.')
        job_status = job.status
        job = session.query(SpiderExecutionQueue).filter_by(id=job.id).first()
        job.status = job_status
        job.update_time = datetime.datetime.now()

        project_storage = ProjectStorage(self.project_storage_dir,
                                         job.spider.project)

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
            #historical_job.log_file = log_file
            import re
            items_crawled_pattern = re.compile(r"\'item_scraped_count\': (\d+),")
            error_log_pattern = re.compile(r"\'log_count/ERROR\': (\d+),")
            warning_log_pattern = re.compile(r"\'log_count/WARNING\': (\d+),")
            log_file.seek(0)
            log_raw = log_file.read()
            log_encoding = chardet.detect(log_raw)['encoding']
            try:
                log_content = ensure_str(log_raw, log_encoding)
                m = items_crawled_pattern.search(log_content)
                if m:
                    historical_job.items_count = int(m.group(1))

                m = error_log_pattern.search(log_content)
                if m and historical_job.status == JOB_STATUS_SUCCESS:
                    historical_job.status = JOB_STATUS_FAIL
                m = warning_log_pattern.search(log_content)
                if m and historical_job.status == JOB_STATUS_SUCCESS:
                    historical_job.status = JOB_STATUS_WARNING
                log_file.seek(0)
            except (UnicodeDecodeError, TypeError):
                # use TypeError when detected log_encoding be null.
                LOGGER.warning('Cannot read unicode in log file.')
                log_file.seek(0)
        #if items_file:
        #    historical_job.items_file = items_file

        if items_file:
            items_file.seek(0)
            project_storage.put_job_data(job, log_file, items_file)

        session.delete(job)
        session.add(historical_job)

        session.commit()
        session.refresh(historical_job)
        self.notify_job_finished(historical_job)

        # send mail
        if historical_job.status == JOB_STATUS_FAIL:
            self.try_send_job_failed_mail(historical_job)

        session.close()
        return historical_job

    def _now(self):
        return datetime.datetime.now()

    def try_send_job_failed_mail(self, job):
        LOGGER.debug('try_send_job_failed_mail')
        job_fail_send_mail = self.config.getboolean('job_fail_send_mail')
        if job_fail_send_mail:
            try:
                mail_sender = MailSender(self.config)
                subject = 'scrapydd job failed'
                to_address = self.config.get('job_fail_mail_receiver')
                content = 'bot:%s \r\nspider:%s \r\n job_id:%s \r\n' % (job.spider.project.name,
                                                                        job.spider_name,
                                                                        job.id)
                mail_sender.send(to_addresses=to_address, subject=subject, content=content)

            except Exception as e:
                LOGGER.error('Error when sending job_fail mail %s' % e)

    def clear_finished_jobs(self):
        job_history_limit_each_spider = 100
        with session_scope() as session:
            spiders = list(session.query(distinct(HistoricalJob.spider_id)))
        for row in spiders:
            spider_id = row[0]
            with session_scope() as session:
                over_limitation_jobs = session.query(HistoricalJob)\
                    .filter_by(spider_id=spider_id)\
                    .order_by(desc(HistoricalJob.complete_time))\
                    .slice(job_history_limit_each_spider, 1000)\
                    .all()
            for over_limitation_job in over_limitation_jobs:
                self._remove_histical_job(over_limitation_job)

    def reset_timeout_job(self):
        KILL_TIMEOUT = 120
        now = self._now()
        with session_scope() as session:
            for job in session.query(SpiderExecutionQueue)\
                    .filter(SpiderExecutionQueue.status == JOB_STATUS_RUNNING):

                # check job time_out expire start
                # the next status is STOPPING then ERROR
                spider = session.query(Spider).get(job.spider_id)
                job_timeout_setting = session.query(SpiderSettings)\
                    .filter_by(spider_id=spider.id,
                               setting_key='timeout').first()
                job_timeout = int(job_timeout_setting.value) \
                    if job_timeout_setting else 3600
                if now > job.start_time + \
                    datetime.timedelta(seconds=job_timeout):
                    job.status = JOB_STATUS_STOPPING
                    job.update_time = self._now()
                    session.add(job)
                    LOGGER.info('Job %s is running timeout, stopping.', job.id)
                    session.commit()
                    continue

                # expire in not updated in a update_timeout.
                # may be node error, restart it
                # the status is PENDING
                if now > job.update_time + datetime.timedelta(minutes=1):
                    job.status = JOB_STATUS_PENDING
                    job.pid = None
                    job.node_id = None
                    job.update_time = self._now()
                    session.add(job)
                    session.commit()
                    LOGGER.info('Job %s is update timeout, reset.', job.id)
                    continue

            for job in session.query(SpiderExecutionQueue)\
                    .filter(SpiderExecutionQueue.status.in_([JOB_STATUS_STOPPING])):
                if (datetime.datetime.now() - job.start_time).seconds > KILL_TIMEOUT:
                    # job is running too long, should be killed
                    historical_job = HistoricalJob()
                    historical_job.id = job.id
                    historical_job.spider_id = job.spider_id
                    historical_job.project_name = job.project_name
                    historical_job.spider_name = job.spider_name
                    historical_job.fire_time = job.fire_time
                    historical_job.start_time = job.start_time
                    historical_job.complete_time = job.update_time
                    historical_job.status = 3
                    session.delete(job)
                    session.add(historical_job)
                    LOGGER.info('Job %s is timeout, killed.' % job.id)
            session.commit()

    def _remove_histical_job(self, job):
        '''
        @type job: HistoricalJob
        '''

        with session_scope() as session:
            job = session.query(HistoricalJob).filter(HistoricalJob.id == job.id).first()
            spider = job.spider
            project = spider.project
            project_storage_dir = self.config.get('project_storage_dir')
            project_storage = ProjectStorage(project_storage_dir, project)
            project_storage.delete_job_data(job)
            session.delete(job)
            session.commit()

    def remove_schedule(self, spider, trigger_id):
        with session_scope() as session:
            trigger = session.query(Trigger)\
                .filter_by(spider_id=spider.id, id=trigger_id).first()

            session.delete(trigger)
            if self.scheduler.get_job(str(trigger_id)):
                self.scheduler.remove_job(str(trigger.id))

        if self.sync_obj:
            LOGGER.info('remove_schedule')
            self.sync_obj.remove_schedule_job(trigger.id)

    def attach_job_observer(self, observer: JobObserver):
        LOGGER.debug('adding observer')
        self._job_observers.append(observer)

    def detach_job_observer(self, observer: JobObserver):
        LOGGER.debug('deattch')
        self._job_observers.remove(observer)

    def notify_job_finished(self, job):
        for observer in self._job_observers:
            observer.on_job_finished(job)


def build_scheduler() -> TornadoScheduler:
    executors = {
        'default': ThreadPoolExecutor(20),
    }
    scheduler = TornadoScheduler(executors=executors)
    return scheduler
