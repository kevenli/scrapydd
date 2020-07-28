from sqlalchemy import *
from migrate import *
from sqlalchemy.ext.declarative import declarative_base
meta = MetaData()
Base = declarative_base(metadata=meta)




class SpiderFigure(Base):
    __tablename__ = 'spider_figure'

    id = Column(Integer, primary_key=True, autoincrement=True)
    spider_id = Column(Integer, ForeignKey('spiders.id'))
    text = Column(String(length=2000))


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    spiders_table = Table('spiders', meta, autoload=True)
    spider_figure_table = meta.tables['spider_figure']
    spider_figure_table.create()

def downgrade(migrate_engine):
    meta.bind = migrate_engine
    spider_figure_table = meta.tables['spider_figure']
    spider_figure_table.drop()
