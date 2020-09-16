from sqlalchemy import *
from migrate import *
meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    node_table = Table('nodes', meta, autoload=True)
    node_table_id = node_table.c.id
    node_table_id.alter(type=BigInteger, autoincrement=True)


def downgrade(migrate_engine):
    # Operations to reverse the above upgrade go here.
    pass
