from sqlalchemy import *
from migrate import *
meta = MetaData()

node_key_table = Table(
    'node_keys', meta,
    Column('id', Integer, primary_key=True),
    Column('key', String(length=50), unique=True, nullable=False),
    Column('secret_key', String(length=50), nullable=False),
    Column('is_deleted', Boolean, default=False),
    Column('used_node_id', Integer),
    Column('create_at', DateTime, nullable=False),
)


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    node_key_table.create()

    nodes_table = Table('nodes', meta, autoload=True)
    nodes_table_key_id = Column('node_key_id', Integer, ForeignKey('node_keys.id'))
    #nodes_table_is_deleted = Column('is_deleted', Boolean, default=False)
    nodes_table_key_id.create(nodes_table)
    #nodes_table_is_deleted.create(nodes_table)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    node_key_table.drop()

    nodes_table = Table('nodes', meta, autoload=True)
    nodes_table.c['node_key_id'].drop()
    #nodes_table.c['is_deleted'].drop()