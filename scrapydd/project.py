from datetime import datetime
import logging
from typing import List
import time
import hashlib
from scrapydd.models import session_scope, ProjectPackage, Project, Spider, \
    Trigger, SpiderExecutionQueue, \
    SpiderParameter, Session, User
from scrapydd.storage import ProjectStorage
from scrapydd.exceptions import ProjectNotFound, SpiderNotFound, \
    ProjectAlreadyExists
from .workspace import SpiderSetting, find_package_version
from .models import Package


logger = logging.getLogger(__name__)


class SpiderNameAlreadyExist(Exception):
    pass


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

    async def upload_project(self, user, project_name, version, eggf):
        with session_scope() as session:
            project = session.query(Project)\
                .filter_by(name=project_name, owner=user).first()

            if project is None:
                project = Project()
                project.name = project_name
                project.storage_version = self.default_project_storage_version
                if user:
                    project.owner_id = user.id
            project.version = version
            session.add(project)
            session.commit()

            ret = await self.upload_project_package(session, project,
                                                    f_egg=eggf,
                                                    version=version,
                                                    auto_populate_spiders=True)
            return ret

    def create_project(self, session, user_id, project_name,
                       return_existing=False):
        if hasattr(user_id, 'id'):
            user_id = user_id.id
        existing_project = session.query(Project)\
            .filter_by(owner_id=user_id, name=project_name).first()

        if existing_project and return_existing:
            return existing_project

        if existing_project:
            raise ProjectAlreadyExists()

        project = Project()
        project.owner_id = user_id
        project.name = project_name
        project.storage_version = self.default_project_storage_version
        session.add(project)
        session.commit()
        return project

    def create_spider(self, session, project, spider_name):
        existing_spiders = session.query(Spider)\
            .filter_by(project_id=project.id)
        for existing_spider in existing_spiders:
            if existing_spider.name == spider_name:
                raise SpiderNameAlreadyExist()

        spider = Spider(project=project, name=spider_name)
        session.add(spider)
        session.commit()
        return spider


    async def upload_project_package(self, session, project, f_egg, version,
                                     auto_populate_spiders=False):
        """
        Upload a new package for an existing project.
        :param session: Session
        :param project: Project
        :param f_egg: file-like obj of project egg binary
        :param version: package version
        :return: the project
        """
        version = str(int(time.time()))
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
        # TODO: identifying egg by custom version should be removed.
        #  we can extract version from egg metadata, and
        #  the uploaded package should be versioned not only by the binary
        #  version tag but also the uploaded time.
        #  As the auto-generated package.id will not be available
        #  before the data persisted, but file_path must be ready
        #  before it, so the file_path cannot be related to the
        #  package.id. Need another unique identifier here.
        egg_file_path = project_storage.put_egg(f_egg, version)
        project.package = package
        session.add(project)
        session.commit()

        if auto_populate_spiders:
            for spider_name in spiders:
                existing_spider = session.query(Spider)\
                    .filter_by(project=project, name=spider_name).first()
                if not existing_spider:
                    new_spider = Spider(project=project, name=spider_name)
                    session.add(new_spider)
        session.commit()

        package = Package()
        package.project = project
        package.type = 'scrapy'
        package.spider_list = ','.join(spiders)
        package.version = self._generate_project_package_version(project)
        f_egg.seek(0)
        package.egg_version = find_package_version(f_egg)
        package.file_path = egg_file_path
        package.create_date = datetime.now()
        f_egg.seek(0)

        package.checksum = self._compute_checksum(f_egg)
        session.add(package)
        session.commit()
        session.refresh(project)
        return project

    def _generate_project_package_version(self, project):
        try:
            last_version = project.packages[0].version
            return last_version + 1
        except IndexError:
            return 1

    def _compute_checksum(self, f_egg):
        f_egg.seek(0)
        h = hashlib.sha1()
        h.update(f_egg.read())
        return h.hexdigest()

    def delete_project(self, user_id, project_id):
        with session_scope() as session:
            project = session.query(Project).get(project_id)
            project_storage = ProjectStorage(
                self.project_storage_dir, project,
                self.default_project_storage_version)
            for spider in project.spiders:
                triggers = session.query(Trigger)\
                    .filter_by(spider_id=spider.id)
                session.query(SpiderExecutionQueue)\
                    .filter_by(spider_id=spider.id).delete()
                session.query(SpiderParameter)\
                    .filter_by(spider_id=spider.id).delete()
                session.commit()
                for trigger in triggers:
                    self.scheduler_manager\
                        .remove_schedule(spider, trigger_id=trigger.id)
                session.query(SpiderExecutionQueue)\
                    .filter_by(spider_id=spider.id).delete()
                for historical_job in spider.historical_jobs:
                    project_storage.delete_job_data(historical_job)
                    session.delete(historical_job)
                session.delete(spider)
            project_storage.delete_egg()
            for package in project.packages:
                self._delete_project_package(session, package)

            if project.package:
                session.delete(project.package)

            session.delete(project)
            session.commit()

    def _delete_project_package(self, session, package):
        session.delete(package)
        session.commit()

    def get_projects(self, session: Session, user: User) -> List[Project]:
        """
        Get projects by owner.
        :param session: pass a session from caller.
        :param user:    the owner
        :return: list of Project
        """
        projects = session.query(Project).filter_by(owner=user)
        return projects

    def get_spider(self, session: Session, user: User,
                   project_id, spider_id) -> Spider:
        project = session.query(Project).filter_by(owner=user,
                                                   id=project_id).first()
        if not project:
            raise ProjectNotFound()

        spider = session.query(Spider).filter_by(project_id=project.id,
                                                 id=spider_id).first()
        if not spider:
            raise SpiderNotFound()

        return spider

    def get_project(self, session: Session, user: User, project_id) -> Project:
        project = session.query(Project).filter_by(owner=user,
                                                   id=project_id).first()
        if not project:
            raise ProjectNotFound()
        return project

    def get_project_by_name(self, session: Session, user: User,
                            project_name) -> Project:
        project = session.query(Project).filter_by(owner=user,
                                                   name=project_name).first()
        return project

    def get_job_figure(self, session: Session,
                       job: SpiderExecutionQueue) -> dict:
        job = session.query(SpiderExecutionQueue).get(job.id)

        if job.spider.figure and job.spider.figure.text:
            figure = SpiderSetting.from_json(job.spider.figure.text)
        else:
            figure = SpiderSetting(job.spider_name)
        figure.spider = job.spider_name
        
        for parameter in job.spider.parameters:
            figure.spider_parameters[parameter.parameter_key] = parameter.value
        return figure

    def get_job_egg(self, session: Session, job: SpiderExecutionQueue):
        project = job.spider.project
        try:
            package = project.packages[0]
            return open(package.file_path, 'rb')
        except IndexError:
            logger.warning('get_job_egg IndexError when retrieving project '
                           'packages.')
            project_storage_dir = self.project_storage_dir
            project_storage = ProjectStorage(project_storage_dir, project)
            version, f_egg = project_storage.get_egg()
            logger.debug('get project version, project id: %s version: %s',
                         project.id, version)
            return f_egg
