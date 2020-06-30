import os
from sqlalchemy.ext.serializer import loads, dumps
from scrapydd.models import init_database, engine, metadata, Session
from scrapydd.models import Project, Spider, User, JobStatus
from scrapydd import models


backup_tables = [
    models.User,
    models.UserKey,
    models.Project,
    models.ProjectPackage,
    models.Spider,
    models.SpiderParameter,
    models.SpiderSettings,
    models.JobStatus,
    models.SpiderWebhook,
    models.Trigger,
    models.Node,
    models.NodeKey,
    models.SpiderExecutionQueue,
    models.HistoricalJob,
    models.SysSpiderPlugin,
    models.SysSpiderPluginParameter,
    models.WebhookJob,
]


def backup():
    if not os.path.exists('backup'):
        os.mkdir('backup')

    #backup_tables = metadata.sorted_tables
    session = Session()
    for table in backup_tables:
        table_name = table.__tablename__
        query = session.query(table)
        objs = query.all()
        serialized = dumps(objs)
        with open(f'backup/{table_name}.dump', 'wb') as f:
            f.write(serialized)
        print(f'dumped {len(objs)} rows from {table_name}')


class BackupCommand:
    def run(self):
        init_database()
        backup()
