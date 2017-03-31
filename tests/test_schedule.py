import unittest
from scrapydd.schedule import SchedulerManager
import logging
from scrapydd.models import Session, HistoricalJob, init_database

class SchedulerManagerTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        init_database()

    @unittest.skip
    def test_clear_finised_jobs(self):
        target = SchedulerManager()
        target._clear_running_jobs()
        jobids = []
        spider_id = 1
        for i in range(0, 101):
            job = target.add_task('weibo', 'discover')
            jobids.append(job.id)
            target.job_start(job.id, 0)
            job.status = 2
            target.job_finished(job)

        jobids = jobids[-100:]
        target.clear_finished_jobs()

        session = Session()
        for job in session.query(HistoricalJob).filter(HistoricalJob.spider_id==spider_id):
            self.assertTrue(job.id in jobids)
        session.close()
