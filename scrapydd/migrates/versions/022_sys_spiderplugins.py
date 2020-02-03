import enum
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
meta = MetaData()
Base = declarative_base(metadata=meta)


class SysSpiderPlugin(Base):
    __tablename__ = 'sys_spiderplugins'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(length=50), nullable=False, unique=True)
    location = Column(String(length=255), nullable=False)


class SpiderPluginParameterDatatype(enum.Enum):
    string = 1
    int = 2
    bool = 3


class SysSpiderPluginParameter(Base):
    __tablename__ = 'sys_spiderpluginparameters'

    id = Column(Integer, primary_key=True, autoincrement=True)
    plugin_id = Column(Integer, ForeignKey('sys_spiderplugins.id'),
                       nullable=False)
    key = Column(String(length=50), nullable=False)
    datatype = Column(Enum(SpiderPluginParameterDatatype), nullable=False)
    required = Column(Boolean, nullable=False)
    default_value = Column(String(length=255))


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    plugins_table = meta.tables['sys_spiderplugins']
    plugins_table.create()

    parameters_table = meta.tables['sys_spiderpluginparameters']
    parameters_table.create()


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    plugins_table = meta.tables['sys_spiderplugins']
    plugins_table.drop()

    parameters_table = meta.tables['sys_spiderpluginparameters']
    parameters_table.drop()
