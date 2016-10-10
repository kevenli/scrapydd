from sqlalchemy import *

def upgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    node = Table('nodes', meta, autoload=True)
    nodes_isalive_col = Column('isalive', Integer, default=1)
    nodes_isalive_col.create(node, pupulate_default=True)

def downgrade(migrate_engine):
    meta = MetaData(bind=migrate_engine)
    node = Table('nodes', meta, autoload=True)
    node.c['isalive'].drop()

