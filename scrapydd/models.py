from sqlalchemy import create_engine, schema, Column
from sqlalchemy.types import Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from migrate.versioning.api import version_control, upgrade
from migrate.exceptions import DatabaseAlreadyControlledError
import os

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

Project.spiders = relationship("Spider", order_by = Spider.id)

class Trigger(Base):
    __tablename__ = 'triggers'

    id = Column(Integer, primary_key=True)
    spider_id = Column(Integer, ForeignKey('spiders.id'))
    cron_pattern = Column(String(length=50))

class SpiderExecutionQueue(Base):
    __tablename__ = 'spider_execution_queue'

    id = Column(String(length=50), primary_key=True)
    spider_id = Column(Integer)
    project_name = Column(String(length=50))
    spider_name = Column(String(length=50))
    fire_time = Column(DateTime)
    start_time = Column(DateTime)
    node_id = Column(Integer)
    status = Column(Integer, default=0)
    update_time = Column(DateTime)
    pid = Column(Integer)

class Node(Base):
    __tablename__ = 'nodes'

    id = Column(Integer, primary_key=True)
    client_ip = Column(String(length=50))
    create_time = Column(DateTime)
    last_heartbeat = Column(DateTime)
    isalive = Column(Integer)


Spider.triggers = relationship("Trigger", order_by = Trigger.id)

def init_database():
    db_url = 'sqlite:///database.db'
    db_repository = os.path.join(os.path.dirname(__file__), 'migrates')
    try:
        version_control(url=db_url, repository=db_repository)
    except DatabaseAlreadyControlledError:
        pass
    upgrade(db_url, db_repository)