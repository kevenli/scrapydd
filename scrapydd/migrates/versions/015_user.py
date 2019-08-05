from sqlalchemy import *
meta = MetaData()

user = Table(
    'user', meta,
    Column('id', Integer, primary_key=True),
    Column('username', String(length=50), nullable=False, unique=True),
    Column('nickname', String(length=50)),
    Column('password', String(length=50)),
    Column('create_at', DateTime),
    Column('last_login', DateTime),
    Column('is_admin', Boolean, default=False)
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    user.create()
    migrate_engine.execute(user.insert().values(id=1, username='admin', password='admin', nickname='admin',
                                                is_admin=True))

def downgrade(migrate_engine):
    meta.bind = migrate_engine
    user.drop()
