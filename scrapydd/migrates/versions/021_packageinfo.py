from sqlalchemy import *
from migrate import *
meta = MetaData()

project_package_table = Table(
    'project_package', meta,
    Column('project_id', Integer, primary_key=True),
    Column('type', String(length=255), nullable=False),
    Column('spider_list', String(length=255), nullable=False),
    Column('settings_module', String(length=255), default=False),
)


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    project_package_table.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    project_package_table.drop()
