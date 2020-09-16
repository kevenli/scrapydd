from sqlalchemy import *
from migrate import *
meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    node_table = Table('nodes', meta, autoload=True)
    node_table_name = Column('name', String(length=30), nullable=True)
    node_table_name.create(node_table)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    node_table = Table('nodes', meta, autoload=True)
    node_table.c['name'].drop()
