from sqlalchemy import *
from migrate import *

meta = MetaData()

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    webhook_jobs = Table('webhook_jobs', meta, autoload=True)
    webhook_jobs_log = Column('log', Text)
    webhook_jobs_log.create(webhook_jobs)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    webhook_jobs = Table('webhook_jobs', meta, autoload=True)
    webhook_jobs.c['log'].drop()
