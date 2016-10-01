from sqlalchemy import create_engine, schema, Column
from sqlalchemy.types import Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
metadata = schema.MetaData()
Base = declarative_base(metadata=metadata)

engine = create_engine('sqlite:///database.db')
Session = sessionmaker(bind=engine)


class Project(Base):
    __tablename__ = 'projects'

    id = Column(Integer, primary_key=True)
    name = Column(String(length=50))
    version = Column(String(length=50))

class Spider(Base):
    __tablename__ = 'spiders'

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('projects.id'))
    project = relationship('Project')
    name = Column(String(length=50))


class Trigger(Base):
    __tablename__ = 'triggers'

    id = Column(Integer, primary_key=True)
    spider_id = Column(Integer, ForeignKey('spiders.id'))
    cron_pattern = Column(String(length=50))

class SpiderExecutionQueue(Base):
    __tablename__ = 'spider_execution_queue'

    id = Column(Integer, primary_key=True)
    spider_id = Column(Integer, unique=True)
    project_name = Column(String(length=50))
    spider_name = Column(String(length=50))
    fire_time = Column(DateTime)
    start_time = Column(DateTime)
    status = Column(Integer, default=0)
    update_time = Column(DateTime)


Spider.triggers = relationship("Trigger", order_by = Trigger.id)

Base.metadata.create_all(engine)