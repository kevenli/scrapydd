from scrapydd.models import Project, ProjectPackage, session_scope
from scrapydd.storage import ProjectStorage
from scrapydd.config import Config


def upgrade():
    config = Config()
    with session_scope() as session:
        for project in session.query(Project):
            if project.package is not None:
                continue
            storage = ProjectStorage(config.get('project_storage_dir'), project)
            eggf = storage.get_egg()
