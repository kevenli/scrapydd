from sqlalchemy import *
from migrate import *
meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    nodekey_table = Table('node_keys', meta, autoload=True)
    nodekey_table_node_id = nodekey_table.c['used_node_id']
    nodekey_table_node_id.alter(type=BigInteger)


def downgrade(migrate_engine):
    # Operations to reverse the above upgrade go here.
    pass
