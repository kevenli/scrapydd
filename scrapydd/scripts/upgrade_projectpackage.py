import logging
from tornado.ioloop import IOLoop
from scrapydd.models import Project, ProjectPackage, session_scope
from scrapydd.storage import ProjectStorage
from scrapydd.config import Config
from scrapydd.workspace import RunnerFactory, ProcessFailed, InvalidProjectEgg


logger = logging.getLogger(__name__)


def upgrade():
    ioloop = IOLoop.current()
    config = Config()
    runner_factory = RunnerFactory(config)
    with session_scope() as session:
        for project in session.query(Project):
            if project.package is not None:
                continue
            storage = ProjectStorage(config.get('project_storage_dir'), project)
            version, eggf = storage.get_egg()
            runner = runner_factory.build(eggf)
            try:
                project_settings_module = ioloop.run_sync(runner.settings_module)
                spider_list = ioloop.run_sync(runner.list)
                package = ProjectPackage()
                package.project = project
                package.type = 'scrapy'
                package.settings_module = project_settings_module
                package.spider_list = ','.join(spider_list)
                session.add(package)
                session.commit()
            except (ProcessFailed, InvalidProjectEgg) as ex:
                logger.error(ex)
