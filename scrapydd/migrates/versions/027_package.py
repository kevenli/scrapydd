from sqlalchemy import *
from migrate import *
from sqlalchemy.ext.declarative import declarative_base
meta = MetaData()
Base = declarative_base(metadata=meta)


class Package(Base):
    __tablename__ = 'packages'

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=False)
    type = Column(String(length=255), nullable=False)
    spider_list = Column(String(length=255), nullable=False)
    version = Column(Integer, nullable=False)
    egg_version = Column(String(length=20))
    checksum = Column(String(length=40), nullable=False)
    file_path = Column(String(length=200), nullable=False)
    create_date = Column(DateTime, nullable=False)

    __table_args__ = (
        UniqueConstraint('project_id', 'version',
                         name='uk_packages_project_id_version'),
    )


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    project_table = Table('projects', meta, autoload=True)
    package_table = meta.tables['packages']
    package_table.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    package_table = meta.tables['packages']
    package_table.drop()
