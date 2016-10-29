from sqlalchemy import *
from migrate import *

meta = MetaData()

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    job_history = Table('job_history', meta, autoload=True)
    job_history_items_count = Column('items_count', Integer)
    job_history_items_count.create(job_history)

def downgrade(migrate_engine):
    meta.bind = migrate_engine

    job_history = Table('job_history', meta, autoload=True)
    job_history.c['items_count'].drop()
