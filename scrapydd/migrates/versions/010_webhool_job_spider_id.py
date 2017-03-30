from sqlalchemy import *
from migrate import *

meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    webhoook_jobs = Table('webhook_jobs', meta, autoload=True)
    webhoook_jobs_spider_id = Column('spider_id', Integer)
    webhoook_jobs_spider_id.create(webhoook_jobs)

def downgrade(migrate_engine):
    webhoook_jobs = Table('webhook_jobs', meta, autoload=True)
    webhoook_jobs.c['spider_id'].drop()