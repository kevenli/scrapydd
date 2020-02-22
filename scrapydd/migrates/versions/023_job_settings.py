from sqlalchemy import *
from migrate import *
meta = MetaData()



def upgrade(migrate_engine):
    meta.bind = migrate_engine
    job_table = Table('spider_execution_queue', meta, autoload=True)
    job_settings = Column('settings', Text, nullable=True)
    job_settings.create(job_table)

def downgrade(migrate_engine):
    meta.bind = migrate_engine
    job_table = Table('spider_execution_queue', meta, autoload=True)
    job_table.c['settings'].drop()
