from sqlalchemy import *
from migrate import *

meta = MetaData()

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    nodes = Table('nodes', meta, autoload=True)
    nodes_tags = Column('tags', String(length=200))
    nodes_tags.create(nodes)

def downgrade(migrate_engine):
    meta.bind = migrate_engine
    nodes = Table('nodes', meta, autoload=True)
    nodes.c['tags'].drop()
