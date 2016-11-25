from sqlalchemy import *
from migrate import *

meta = MetaData()

jobStatus = Table(
    'job_status', meta,
    Column('id', Integer, primary_key=True),
    Column('name', String(length=50), nullable=False),
    Column('desc', String(length=200)),
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    jobStatus.create()
    migrate_engine.execute(jobStatus.insert().values(id = 0, name='pending',desc='job is not started'))
    migrate_engine.execute(jobStatus.insert().values(id = 1, name='running', desc='job is running'))
    migrate_engine.execute(jobStatus.insert().values(id = 2, name='success', desc='job is finished successfully'))
    migrate_engine.execute(jobStatus.insert().values(id = 3, name='fail', desc='job is finished with error'))
    migrate_engine.execute(jobStatus.insert().values(id = 4, name='warning',desc= 'job is finished with error or warning log'))


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    jobStatus.drop()
