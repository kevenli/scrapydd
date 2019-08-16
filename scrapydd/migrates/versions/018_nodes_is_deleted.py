from sqlalchemy import *
from migrate import *
meta = MetaData()

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    nodes_table = Table('nodes', meta, autoload=True)
    nodes_table_is_deleted = Column('is_deleted', Integer, default=False)
    nodes_table_is_deleted.create(nodes_table)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    nodes_table = Table('nodes', meta, autoload=True)
    nodes_table.c['is_deleted'].drop()
