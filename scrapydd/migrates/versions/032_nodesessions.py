from sqlalchemy import *
from migrate import *
from sqlalchemy.ext.declarative import declarative_base
meta = MetaData()
Base = declarative_base(metadata=meta)


class NodeSession(Base):
    __tablename__ = 'nodesessions'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    node_id = Column(BigInteger, ForeignKey('nodes.id'), nullable=False)
    create_at = Column(DateTime, nullable=False)
    last_heartbeat = Column(DateTime, nullable=False)


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    node_table = Table('nodes', meta, autoload=True)
    nodesession_table = meta.tables['nodesessions']
    nodesession_table.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    nodesession_table = meta.tables['nodesessions']
    nodesession_table.drop()
