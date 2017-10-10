from sqlalchemy import *
from migrate import *

meta = MetaData()

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    spider_execution_queue = Table('spider_execution_queue', meta, autoload=True)
    spider_execution_queue_tag = Column('tag', String(length=50))
    spider_execution_queue_tag.create(spider_execution_queue)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    spider_execution_queue = Table('spider_execution_queue', meta, autoload=True)
    spider_execution_queue.c['tag'].drop()
