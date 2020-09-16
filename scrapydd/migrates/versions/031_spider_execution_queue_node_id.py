from sqlalchemy import *
from migrate import *
meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    queue_table = Table('spider_execution_queue', meta, autoload=True)
    queue_table_node_id = queue_table.c['node_id']
    queue_table_node_id.alter(type=BigInteger)


def downgrade(migrate_engine):
    # Operations to reverse the above upgrade go here.
    pass
