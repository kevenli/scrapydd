from sqlalchemy import *
from migrate import *
meta = MetaData()



def upgrade(migrate_engine):
    meta.bind = migrate_engine
    projects_table = Table('projects', meta, autoload=True)
    storage_version = Column('storage_version', Integer, default=1)
    storage_version.create(projects_table)

def downgrade(migrate_engine):
    meta.bind = migrate_engine
    projects_table = Table('projects', meta, autoload=True)
    projects_table.c['storage_version'].drop()
