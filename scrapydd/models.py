from sqlalchemy import create_engine, schema, Column
from sqlalchemy.types import Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from migrate.versioning.api import version_control, upgrade
from migrate.exceptions import DatabaseAlreadyControlledError
import os
from contextlib import contextmanager

metadata = schema.MetaData()
Base = declarative_base(metadata=metadata)

#database_url = 'mysql://scrapydd:scrapydd@localhost/scrapydd'
database_url = 'sqlite:///database.db'
engine = create_engine(database_url)
Session = sessionmaker(bind=engine, expire_on_commit=False)


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

Spider.triggers = relationship("Trigger", order_by=Trigger.id)

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


class HistoricalJob(Base):
    __tablename__ = 'job_history'

    id = Column(String(length=50), primary_key=True)
    spider_id = Column(Integer)
    project_name = Column(String(length=50))
    spider_name = Column(String(length=50))
    fire_time = Column(DateTime)
    start_time = Column(DateTime)
    complete_time = Column(DateTime)
    status = Column(Integer, default=0)
    log_file = Column(String(500))
    items_file = Column(String(500))


class SpiderWebhook(Base):
    __tablename__ = 'spider_webhook'

    id = Column(String(length=50), primary_key=True)
    payload_url = Column(String(length=250))


class WebhookJob(Base):
    __tablename__ = 'webhook_jobs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(length=50))
    payload_url = Column(String(length=50))
    items_file = Column(String(length=250))
    status = Column(Integer)


def init_database():
    db_repository = os.path.join(os.path.dirname(__file__), 'migrates')
    try:
        version_control(url=database_url, repository=db_repository)
    except DatabaseAlreadyControlledError:
        pass
    upgrade(database_url, db_repository)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
