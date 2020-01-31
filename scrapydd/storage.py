import os
from shutil import copyfileobj
from os import path
from glob import glob
from distutils.version import LooseVersion
from .exceptions import EggFileNotFound
from shutil import copyfileobj, rmtree
import re

class ProjectStorage:
    '''


    '''
    def __init__(self, data_dir, project, storage_version=None):
        self.project = project

        project_storage_version = project.storage_version
        if storage_version is not None:
            project_storage_version = storage_version

        # for old version of 1, there is no data_dir config, so the data_dir must be current running folder '.'
        if project_storage_version == 1:
            data_dir = '.'

        if project_storage_version == 1:
            self.storage_provider = ProjectStoragePathV1(data_dir)
        elif project_storage_version == 2:
            self.storage_provider = ProjectStoragePathV2(data_dir)
        else:
            raise Exception('Not supported project_storage_version : %s' % project_storage_version)

    def put_egg(self, eggf, version):
        eggf.seek(0)
        egg_file_dir = self.storage_provider.get_project_eggs_dir(self.project)
        egg_file_path = self._eggpath(version)
        if not os.path.exists(egg_file_dir):
            os.makedirs(egg_file_dir)
        with open(egg_file_path, 'wb+') as fdst:
            copyfileobj(eggf, fdst)

    def delete_egg(self, version=None):
        egg_file_dir = self.storage_provider.get_project_eggs_dir(self.project)
        if version is None:
            versions = self.list_egg_versions()
        else:
            versions = [version]
        for exact_version in versions:
            egg_file_path = self._eggpath(exact_version)
            os.remove(egg_file_path)

    def get_egg(self, version=None):
        eggs_dir = self.storage_provider.get_project_eggs_dir(self.project)
        if version is None:
            try:
                version = self.list_egg_versions()[-1]
            except IndexError:
                return None, None
        egg_file_path = self._eggpath(version)
        if not path.exists(egg_file_path):
            raise EggFileNotFound()
        return version, open(egg_file_path, 'rb')

    def _eggpath(self, version):
        sanitized_version = re.sub(r'[^a-zA-Z0-9_-]', '_', version)
        return path.join(self.storage_provider.get_project_eggs_dir(self.project),
                         '%s.egg' % sanitized_version)

    def list_egg_versions(self):
        eggdir = self.storage_provider.get_project_eggs_dir(self.project)
        versions = [path.splitext(path.basename(x))[0] \
            for x in glob("%s/*.egg" % eggdir)]
        return sorted(versions, key=LooseVersion)

    def put_job_data(self, job, log_file, item_file):
        log_file_path = self.storage_provider.get_job_log_path(job)
        log_file_dir = path.dirname(log_file_path)
        if not path.exists(log_file_dir):
            os.makedirs(log_file_dir)
        if log_file:
            log_file.seek(0)
            with open(log_file_path, 'wb+') as f_log:
                copyfileobj(log_file, f_log)

        items_file_path = self.storage_provider.get_job_item_path(job)
        items_file_dir = path.dirname(items_file_path)
        if not path.exists(items_file_dir):
            os.makedirs(items_file_dir)

        if item_file:
            item_file.seek(0)
            with open(items_file_path, 'wb+') as f_items:
                copyfileobj(item_file, f_items)

    def delete_job_data(self, job):
        log_file_path = self.storage_provider.get_job_log_path(job)

        if path.exists(log_file_path):
            os.remove(log_file_path)
        log_file_dir = path.dirname(log_file_path)
        if path.exists(log_file_dir) and not os.listdir(log_file_dir):
            os.rmdir(log_file_dir)

        items_file_path = self.storage_provider.get_job_item_path(job)

        if path.exists(items_file_path):
            os.remove(items_file_path)
        items_file_dir = path.dirname(items_file_path)
        if path.exists(items_file_dir) and not(os.listdir(items_file_dir)):
            os.rmdir(items_file_dir)


    def get_job_log(self, job):
        log_file_path = self.storage_provider.get_job_log_path(job)
        return open(log_file_path, 'rb')

    def get_job_items(self, job):
        items_file_path = self.storage_provider.get_job_item_path(job)
        return open(items_file_path, 'rb')

    def upgrade_project(self, project):
        pass


class ProjectStoragePathV1():
    '''
    The old scrapyd style data storage
    '''
    def __init__(self, data_dir):
        self.data_dir = data_dir

    def get_project_eggs_dir(self, project):
        return os.path.join(self.data_dir, 'eggs', project.name)

    def get_job_item_path(self, job):
        return os.path.join(self.data_dir, 'items', job.spider.project.name, job.spider.name, '%s.jl' % job.id)

    def get_job_log_path(self, job):
        return os.path.join(self.data_dir, 'logs', job.spider.project.name, job.spider.name, '%s.log' % job.id)


class ProjectStoragePathV2():
    '''
    The new storage for multi-tenant support
    '''
    def __init__(self, data_dir):
        self.data_dir = data_dir

    def get_project_eggs_dir(self, project):
        return os.path.join(self.data_dir, 'projects', '%d' % (project.id), 'eggs')

    def get_job_item_path(self, job):
        return os.path.join(self.data_dir, 'projects', '%d' % (job.spider.project.id), 'spiders',
                            job.spider.name, 'jobs/%s' % job.id, 'items.jl')

    def get_job_log_path(self, job):
        return os.path.join(self.data_dir, 'projects', '%d' % (job.spider.project.id), 'spiders',
                            job.spider.name, 'jobs/%s' % job.id, 'job.log')
