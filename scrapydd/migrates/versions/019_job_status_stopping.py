from sqlalchemy import *
from migrate import *

meta = MetaData()

jobStatus = Table(
    'job_status', meta,
    Column('id', Integer, primary_key=True, autoincrement=False),
    Column('name', String(length=50), nullable=False),
    Column('desc', String(length=200)),
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    migrate_engine.execute(jobStatus.insert().values(id = 5, name='stopping',desc= 'job is finished with error or warning log'))


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    migrate_engine.execute(jobStatus.delete().where(jobStatus.c.id==5))
