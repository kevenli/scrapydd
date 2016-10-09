from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime

meta = MetaData()

project = Table(
    'projects', meta,
    Column('id', Integer, primary_key=True),
    Column('name', String(length=50)),
    Column('version', String(length=50)),
)

spider  = Table(
    'spiders', meta,
    Column('id', Integer, primary_key=True),
    Column('project_id', Integer),
    Column('name', String(length=50)),
)

trigger = Table(
    'triggers', meta,
    Column('id', Integer, primary_key=True),
    Column('spider_id', Integer),
    Column('cron_pattern', String(length=50)),
)

spider_executing_queue = Table(
    'spider_execution_queue', meta,
    Column('id', String(length=50), primary_key=True),
    Column('spider_id', Integer),
    Column('project_name', String(length=50)),
    Column('spider_name', String(length=50)),
    Column('fire_time', DateTime),
    Column('start_time', DateTime),
    Column('node_id', Integer),
    Column('status', Integer, default=0),
    Column('update_time', DateTime),
)

node = Table(
    'nodes', meta,
    Column('id', Integer, primary_key=True),
    Column('client_ip', String(length=50)),
    Column('create_time', DateTime),
    Column('last_heartbeat', DateTime),
)


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    project.create()
    spider.create()
    trigger.create()
    spider_executing_queue.create()
    node.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    project.drop()
    spider.drop()
    trigger.drop()
    spider_executing_queue.drop()
    node.drop()