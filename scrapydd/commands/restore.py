import os
from sqlalchemy.ext.serializer import loads, dumps
from scrapydd.models import init_database, engine, metadata, Session
from scrapydd import models
from .backup import backup_tables


def restore():
    if not os.path.exists('backup'):
        os.mkdir('backup')

    session = Session()
    for table in backup_tables:
        table_name = table.__tablename__
        with open(f'backup/{table_name}.dump', 'rb') as f:
            serialized = f.read()
        # print(serialized)
        loaded_objs = loads(serialized, metadata, Session);
        loaded_objs_len = len(loaded_objs)
        for obj in loaded_objs:
            # print(obj)
            session.merge(obj)
        session.commit()
        print(f'{loaded_objs_len} objs loaded into {table_name}.')


class RestoreCommand:
    def run(self):
        init_database()
        restore()


