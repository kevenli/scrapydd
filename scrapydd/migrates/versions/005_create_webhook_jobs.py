from sqlalchemy import *
from migrate import *

meta = MetaData()

webhook_jobs = Table(
    'webhook_jobs', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('job_id', String(length=50)),
    Column('payload_url', String(length=50)),
    Column('items_file', String(length=250)),
    Column('status', Integer),
)

spider_webhook = Table(
    'spider_webhook', meta,
    Column('id',String(length=50), primary_key=True),
    Column('payload_url', String(length=250)),
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    webhook_jobs.create()
    spider_webhook.create()

    job_history = Table('job_history', meta, autoload=True)
    job_history_log_file = Column('log_file', String(length=500))
    job_history_items_file = Column('items_file', String(length=500))
    job_history_log_file.create(job_history)
    job_history_items_file.create(job_history)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    webhook_jobs.drop()
    spider_webhook.drop()

    job_history = Table('job_history', meta, autoload=True)
    job_history.c['log_file'].drop()
    job_history.c['items_file'].drop()

