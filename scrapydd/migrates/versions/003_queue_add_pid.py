from sqlalchemy import *
from migrate import *


def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    table = Table('spider_execution_queue', meta, autoload=True)
    pid_col = Column('pid', Integer)
    pid_col.create(table)


def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    table = Table('spider_execution_queue', meta, autoload=True)
    table.c['pid'].drop()
