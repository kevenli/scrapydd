from sqlalchemy import *
from migrate import *

meta = MetaData()

historical_job = Table(
    'job_history', meta,
    Column('id', String(length=50), primary_key=True),
    Column('spider_id', Integer),
    Column('project_name', String(length=50)),
    Column('spider_name', String(length=50)),
    Column('fire_time', DateTime),
    Column('start_time', DateTime),
    Column('complete_time', DateTime),
    Column('status', Integer, default=0)
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    historical_job.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    historical_job.drop()
