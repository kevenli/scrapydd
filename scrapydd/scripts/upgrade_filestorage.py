'''
This script is to upgrade project's files from old-style (storage_version 1)
to new-style (storage_version 2), to well support multi-tenant use.
This procedure is forced to run on server start.
On server start:
1 Check all project's storage_version
2 If it is "1", upgrade it to 2:
2.1 move all files into new-style place
2.2 set project's storage_version to 2
3 Continue run
add in version 0.6.6
'''
from ..models import Project, session_scope
from ..storage import ProjectStorage
from ..config import Config

import logging

logger = logging.getLogger(__name__)

def upgrade(config=None):
    if config is None:
        config = Config()
    logger.info('Start upgrading projects storage')
    with session_scope() as session:
        for project in session.query(Project).all():
            if project.storage_version == 1:
                _move_project_files(project, config)
                project.storage_version = 2
                session.add(project)
                session.commit()

def _move_project_files(project, config):
    old_project_storage = ProjectStorage(data_dir='.', project=project)
    new_project_storage = ProjectStorage(data_dir=config.get('project_storage_dir'), project=project, storage_version=2)

    egg_version, eggf = old_project_storage.get_egg()
    new_project_storage.put_egg(eggf, egg_version)

    for spider in project.spiders:
        for job in spider.historical_jobs:
            try:
                job_items = old_project_storage.get_job_items(job)
            except IOError:
                job_items = None
            try:
                job_log = old_project_storage.get_job_log(job)
            except IOError:
                job_log = None
            new_project_storage.put_job_data(job, log_file=job_log, item_file=job_items)

