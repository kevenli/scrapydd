from sqlalchemy import create_engine, schema, Column, desc
from sqlalchemy.types import Integer, String, DateTime, Text, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from migrate.versioning.api import version_control, upgrade
from migrate.exceptions import DatabaseAlreadyControlledError
import os
from contextlib import contextmanager
from .config import Config

metadata = schema.MetaData()
Base = declarative_base(metadata=metadata)

#database_url = 'mysql://scrapydd:scrapydd@localhost/scrapydd'
database_url = 'sqlite:///database.db'
engine = create_engine(database_url)
Session = sessionmaker(bind=engine, expire_on_commit=False)
_Session = None

class User(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String(length=50), nullable=False, unique=True)
    nickname = Column(String(length=50))
    password = Column(String(length=50))
    create_at = Column(DateTime)
    last_login = Column(DateTime)
    is_admin = Column(Boolean, default=False)


class UserKey(Base):
    __tablename__ = 'user_keys'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('user.id'))
    user = relationship('User')
    app_key = Column(String(length=50), nullable=False, unique=True)
    app_secret = Column(String(length=50))
    enabled = Column(Boolean, default=True)
    remark = Column(String(length=255))
    create_at = Column(DateTime)


class Project(Base):
    __tablename__ = 'projects'

    id = Column(Integer, primary_key=True)
    name = Column(String(length=50))
    version = Column(String(length=50))
    storage_version = Column(Integer, nullable=False)


class ProjectPackage(Base):
    __tablename__ = 'project_package'

    project_id = Column(Integer, ForeignKey('projects.id'), primary_key=True)
    type = Column(String(length=255), nullable=False)
    spider_list = Column(String(length=255), nullable=False)
    settings_module = Column(String(length=255), default=False)
    project = relationship("Project")


Project.package = relationship("ProjectPackage", uselist=False)


class Spider(Base):
    __tablename__ = 'spiders'

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('projects.id'))
    project = relationship('Project')
    name = Column(String(length=50))
    settings = relationship("SpiderSettings",
                         collection_class=attribute_mapped_collection('setting_key'))

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
    spider_id = Column(Integer, ForeignKey('spiders.id'))
    spider = relationship('Spider')
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
    is_deleted = Column(Boolean, default=False)
    node_key_id = Column(Integer, ForeignKey('node_keys.id'))


class NodeKey(Base):
    __tablename__ = 'node_keys'

    id = Column(Integer, primary_key=True)
    key = Column(String(length=50), unique=True, nullable=False)
    secret_key = Column(String(length=50), nullable=False)
    is_deleted = Column(Boolean, default=False)
    used_node_id = Column(Integer)
    create_at = Column(DateTime, nullable=False)


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
    job_id = Column(String(length=50), ForeignKey('job_history.id'))
    job = relationship('HistoricalJob')
    spider_id = Column(Integer, ForeignKey('spiders.id'))
    payload_url = Column(String(length=50))
    items_file = Column(String(length=250))
    status = Column(Integer, ForeignKey('job_status.id'), default=0)
    status_obj = relationship('JobStatus')
    log = Column(Text)

HistoricalJob.webhook_job = relationship('WebhookJob', uselist=False)



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
