from sqlalchemy import *
from migrate import *
meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    user_table = Table('user', meta, autoload=True)
    user_is_active = Column('is_active', INTEGER, default=1, nullable=True)
    user_is_active.create(user_table)

    conn = migrate_engine.connect()
    update_stmt = user_table.update().values(is_active=1)
    conn.execute(update_stmt)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    user_table = Table('user', meta, autoload=True)
    user_table.c['is_active'].drop()
