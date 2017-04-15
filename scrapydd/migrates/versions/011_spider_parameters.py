from sqlalchemy import *
from migrate import *

meta = MetaData()

spider_parameters = Table(
    'spider_parameters', meta,
    Column('id', Integer, primary_key=True),
    Column('spider_id', Integer),
    Column('parameter_key', String(length=50), nullable=False),
    Column('value', String(length=200)),
)

def upgrade(migrate_engine):
    meta.bind = migrate_engine
    spider_parameters.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    spider_parameters.drop()
