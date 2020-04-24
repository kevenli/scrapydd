from sqlalchemy import *
meta = MetaData()


def upgrade(migrate_engine):
    meta.bind = migrate_engine
    project_table = Table('projects', meta, autoload=True)
    project_owner_id = Column('owner_id', Integer, nullable=True)
    project_owner_id.create(project_table)

    user_table = Table('user', meta, autoload=True)
    conn = migrate_engine.connect()
    result = conn.execute('select id from user')
    row = result.fetchone()
    user_id = row['id']

    update_stmt = project_table.update().values(owner_id=user_id)
    conn.execute(update_stmt)


def downgrade(migrate_engine):
    meta.bind = migrate_engine
    project_table = Table('projects', meta, autoload=True)
    project_table.c['owner_id'].drop()
