import logging
from typing import List
from tornado.gen import coroutine, Return
from scrapydd.workspace import RunnerFactory
from scrapydd.models import session_scope, ProjectPackage, Project, Spider, Trigger, SpiderExecutionQueue, \
    SpiderParameter, Session, User
from scrapydd.storage import ProjectStorage
from scrapydd.exceptions import ProjectNotFound, SpiderNotFound, ProjectAlreadyExists


logger = logging.getLogger(__name__)


class ProjectManager:
    def __init__(self, runner_factory,
                 project_storage_dir,
                 scheduler_manager,
                 default_project_storage_version=2,
                 ):
        self.runner_factory = runner_factory
        self.project_storage_dir = project_storage_dir
        self.default_project_storage_version = default_project_storage_version
        self.scheduler_manager = scheduler_manager

    @coroutine
    def upload_project(self, user, project_name, version, eggf):
        runner = self.runner_factory.build(eggf)
        try:
            spiders = yield runner.list()
            logger.debug('spiders: %s' % spiders)
        finally:
            runner.clear()

        with session_scope() as session:
            project = session.query(Project).filter_by(name=project_name).first()

            if project is None:
                project = Project()
                project.name = project_name
                project.storage_version = self.default_project_storage_version
                if user:
                    project.owner_id = user.id
            project.version = version
            session.add(project)
            package = project.package
            if not package:
                package = ProjectPackage()
                package.project = project
            package.type = 'scrapy'
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

    def create_project(self, session, user, project_name):
        existing_project = session.query(Project)\
            .filter_by(owner=user, name=project_name).first()
        if existing_project:
            raise ProjectAlreadyExists()

        project = Project()
        project.owner = user
        project.name = project_name
        project.storage_version = self.default_project_storage_version
        session.add(project)
        session.commit()
        session.refresh(project)
        return project


    async def upload_project_package(self, session, project, f_egg, version, auto_populate_spiders=False):
        """
        Upload a new package for an existing project.
        :param session: Session
        :param project: Project
        :param f_egg: file-like obj of project egg binary
        :param version: package version
        :return: the project
        """
        runner = self.runner_factory.build(f_egg)
        try:
            spiders = await runner.list()
            logger.debug('spiders: %s' % spiders)
        finally:
            runner.clear()

        package = project.package
        if not package:
            package = ProjectPackage()
            package.project = project
        package.type = 'scrapy'
        package.spider_list = ','.join(spiders)
        project_storage = ProjectStorage(self.project_storage_dir, project)
        f_egg.seek(0)
        project_storage.put_egg(f_egg, version)
        session.add(package)
        session.flush()

        if auto_populate_spiders:
            for spider_name in spiders:
                existing_spider = session.query(Spider).filter_by(project=project, name=spider_name).first()
                if not existing_spider:
                    new_spider = Spider(project=project, name=spider_name)
                    session.add(new_spider)
            session.commit()

        session.refresh(project)
        return project


    def delete_project(self, user_id, project_id):
        with session_scope() as session:
            project = session.query(Project).get(project_id)
            project_storage = ProjectStorage(self.project_storage_dir, project,
                                             self.default_project_storage_version)
            for spider in project.spiders:
                triggers = session.query(Trigger).filter_by(spider_id=spider.id)
                session.query(SpiderExecutionQueue).filter_by(spider_id=spider.id).delete()
                session.query(SpiderParameter).filter_by(spider_id=spider.id).delete()
                session.commit()
                for trigger in triggers:
                    self.scheduler_manager.remove_schedule(spider, trigger_id=trigger.id)
                session.query(SpiderExecutionQueue).filter_by(spider_id=spider.id).delete()
                for historical_job in spider.historical_jobs:
                    project_storage.delete_job_data(historical_job)
                    session.delete(historical_job)
                session.delete(spider)
            project_storage.delete_egg()
            if project.package:
                session.delete(project.package)
            session.delete(project)

    def get_projects(self, session: Session, user: User) -> List[Project]:
        """
        Get projects by owner.
        :param session: pass a session from caller.
        :param user:    the owner
        :return: list of Project
        """
        projects = session.query(Project).filter_by(owner=user)
        return projects

    def get_spider(self, session: Session, user: User, project_id, spider_id) -> Spider:
        project = session.query(Project).filter_by(owner=user, id=project_id).first()
        if not project:
            raise ProjectNotFound()

        spider = session.query(Spider).filter_by(project_id=project.id, id=spider_id).first()
        if not spider:
            return SpiderNotFound()

        return spider

    def get_project(self, session: Session, user: User, project_id) -> Project:
        project = session.query(Project).filter_by(owner=user, id=project_id).first()
        if not project:
            raise ProjectNotFound()
        return project

    def get_project_by_name(self, session: Session, user: User, project_name) -> Project:
        project = session.query(Project).filter_by(owner=user, name=project_name).first()
        if not project:
            raise ProjectNotFound()
        return project
