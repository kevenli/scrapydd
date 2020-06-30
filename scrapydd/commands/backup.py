import os
from sqlalchemy.ext.serializer import loads, dumps
from scrapydd.models import init_database, engine, metadata, Session
from scrapydd.models import Project, Spider, User


def backup():
    if not os.path.exists('backup'):
        os.mkdir('backup')
    backup_tables = [
        User,
        Project,
        Spider,
    ]
    session = Session()
    for table in backup_tables:
        table_name = table.__tablename__
        query = session.query(table)
        serialized = dumps(query.all())
        with open(f'backup/{table_name}.dump', 'wb') as f:
            f.write(serialized)


