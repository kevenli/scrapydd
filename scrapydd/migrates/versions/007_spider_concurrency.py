from sqlalchemy import *
from migrate import *
from migrate.changeset.constraint import ForeignKeyConstraint

meta = MetaData()

spider_settings = Table(
    'spider_settings', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('spider_id', Integer),
    Column('setting_key', String(length=50), nullable=False),
    Column('value', String(length=200)),
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    job_queue = Table('spider_execution_queue', meta, autoload=True)
    job_queue_slot = Column('slot', Integer, default=1)
    job_queue_slot.create(job_queue)
    spider_settings.create()

    spiders = Table('spiders', meta, autoload=True)
    spider_settings_spider_id_fk =  ForeignKeyConstraint([spider_settings.c.spider_id], [spiders.c.id])
    spider_settings_spider_id_fk.create()

def downgrade(migrate_engine):
    meta.bind = migrate_engine
    job_queue = Table('spider_execution_queue', meta, autoload=True)
    job_queue.c['slot'].drop()
    spider_settings.drop()
