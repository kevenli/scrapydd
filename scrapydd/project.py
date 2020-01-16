import logging
from tornado.gen import coroutine, Return
from scrapydd.workspace import RunnerFactory
from scrapydd.models import session_scope, ProjectPackage, Project, Spider
from scrapydd.storage import ProjectStorage


logger = logging.getLogger(__name__)


class ProjectManager:
    def __init__(self, runner_factory,
                 project_storage_dir,
                 default_project_storage_version=2,
                 ):
        self.runner_factory = runner_factory
        self.project_storage_dir = project_storage_dir
        self.default_project_storage_version = default_project_storage_version

    @coroutine
    def upload_project(self, user_id, project_name, version, eggf):
        runner = self.runner_factory.build(eggf)
        try:
            spiders = yield runner.list()
            logger.debug('spiders: %s' % spiders)
            project_settings_module = yield runner.settings_module()
        finally:
            runner.clear()

        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()

            if project is None:
                project = Project()
                project.name = project_name
                project.storage_version = self.default_project_storage_version
            project.version = version
            session.add(project)
            package = project.package
            if not package:
                package = ProjectPackage()
                package.project = project
            package.type = 'scrapy'
            package.settings_module = project_settings_module
            package.spider_list = ','.join(spiders)
            session.add(package)
            session.flush()
            project_storage = ProjectStorage(self.project_storage_dir, project)
            project_storage.put_egg(eggf, version)
            session.refresh(project)

            for spider_name in spiders:
                spider = session.query(Spider).filter_by(project_id=project.id, name=spider_name).first()
                if spider is None:
                    spider = Spider()
                    spider.name = spider_name
                    spider.project_id = project.id
                    session.add(spider)
                    session.commit()
                    session.refresh(spider)

            session.commit()
        raise Return(project)