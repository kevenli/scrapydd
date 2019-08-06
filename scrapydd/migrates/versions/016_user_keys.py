from sqlalchemy import *
from migrate import *
from sqlalchemy.ext.declarative import declarative_base

meta = MetaData()
Base = declarative_base(metadata=meta)

class UserKey(Base):
    __tablename__ = 'user_keys'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('user.id'))
    app_key = Column(String(length=50), nullable=False, unique=True)
    app_secret = Column(String(length=50))
    enabled = Column(Boolean, default=True)
    remark = Column(String(length=255))
    create_at = Column(DateTime)


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    user = Table('user', meta, autoload=True)
    user_key_table = Table('user_keys', meta, autoload=True)
    user_key_table.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    user_key_table = Table('user_keys', meta, autoload=True)
    user_key_table.drop()
