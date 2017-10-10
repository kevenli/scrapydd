from sqlalchemy import create_engine, schema, Column, desc
from sqlalchemy.types import Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from migrate.versioning.api import version_control, upgrade
from migrate.exceptions import DatabaseAlreadyControlledError
import os
from contextlib import contextmanager
from scrapydd.config import Config

metadata = schema.MetaData()
Base = declarative_base(metadata=metadata)

#database_url = 'mysql://scrapydd:scrapydd@localhost/scrapydd'
database_url = 'sqlite:///database.db'
engine = create_engine(database_url)
Session = sessionmaker(bind=engine, expire_on_commit=False)
_Session = None


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


class JobStatus(Base):
    __tablename__ = 'job_status'
    id = Column(Integer, primary_key=True)
    name = Column(String(length=50), nullable=False)
    desc = Column(String(length=200))


class SpiderExecutionQueue(Base):
    __tablename__ = 'spider_execution_queue'

    id = Column(String(length=50), primary_key=True)
    spider_id = Column(Integer)
    slot = Column(Integer, default=1)
    project_name = Column(String(length=50))
    spider_name = Column(String(length=50))
    fire_time = Column(DateTime)
    start_time = Column(DateTime)
    node_id = Column(Integer)
    status = Column(Integer, ForeignKey('job_status.id'), default=0)
    status_obj = relationship('JobStatus')
    update_time = Column(DateTime)
    pid = Column(Integer)
    tag = Column(String(length=50))

class Node(Base):
    __tablename__ = 'nodes'

    id = Column(Integer, primary_key=True)
    client_ip = Column(String(length=50))
    create_time = Column(DateTime)
    last_heartbeat = Column(DateTime)
    isalive = Column(Integer)
    tags = Column(String(length=200))


class HistoricalJob(Base):
    __tablename__ = 'job_history'

    id = Column(String(length=50), primary_key=True)
    spider_id = Column(Integer, ForeignKey('spiders.id'))
    spider = relationship('Spider')
    project_name = Column(String(length=50))
    spider_name = Column(String(length=50))
    fire_time = Column(DateTime)
    start_time = Column(DateTime)
    complete_time = Column(DateTime)
    status = Column(Integer, ForeignKey('job_status.id'), default=0)
    status_obj = relationship('JobStatus')
    log_file = Column(String(500))
    items_file = Column(String(500))
    items_count = Column(Integer)

Spider.historical_jobs = relationship("HistoricalJob", order_by=desc(HistoricalJob.start_time))


class SpiderWebhook(Base):
    __tablename__ = 'spider_webhook'

    id = Column(String(length=50), primary_key=True)
    payload_url = Column(String(length=250))


class WebhookJob(Base):
    __tablename__ = 'webhook_jobs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(length=50))
    spider_id = Column(Integer, ForeignKey('spiders.id'))
    payload_url = Column(String(length=50))
    items_file = Column(String(length=250))
    status = Column(Integer, ForeignKey('job_status.id'), default=0)
    status_obj = relationship('JobStatus')


class SpiderSettings(Base):
    __tablename__ = 'spider_settings'

    id = Column(Integer, primary_key=True, autoincrement=True)
    spider_id = Column(Integer, ForeignKey('spiders.id'))
    setting_key = Column(String(length=50), nullable=False)
    value = Column(String(length=200))


class SpiderParameter(Base):
    __tablename__ = 'spider_parameters'

    id = Column(Integer, primary_key=True, autoincrement=True)
    spider_id = Column(Integer, ForeignKey('spiders.id'))
    parameter_key = Column(String(length=50), nullable=False)
    value = Column(String(length=200))

Spider.parameters = relationship('SpiderParameter', order_by=SpiderParameter.parameter_key)


def init_database(config=None):
    if config is None:
        config = Config()

    global database_url
    global engine
    global _Session
    database_url = config.get('database_url')
    engine = create_engine(database_url)
    _Session = sessionmaker(bind=engine, expire_on_commit=False)
    #global Session
    db_repository = os.path.join(os.path.dirname(__file__), 'migrates')
    try:
        version_control(url=database_url, repository=db_repository)
    except DatabaseAlreadyControlledError:
        pass
    upgrade(database_url, db_repository)

def _make_session():
    global _Session
    return _Session()

Session = _make_session



@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = _make_session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
