import os

class ProjectStorage:
    def __init__(self, data_dir, project):
        self.project = project
        self.base_dir = os.path.join(data_dir, str(project.id))

    def put_egg(self, eggf, spider=None, version=None):
        pass

    def get_project_egg(self, spider, version=None):
        pass

    def put_job_data(self, job, log_file, item_file):
        pass

    def get_job_log(self, job):
        pass

    def get_job_items(self, job):
        pass

    def get_job_egg(self, job):
        pass


class StoragePathGeneratorV1():
    '''
    The old scrapyd style data storage
    '''
    def __init__(self, data_dir):
        self.data_dir = data_dir

    def get_spider_egg_path(self,  spider, version):
        return os.path.join(self.data_dir, 'eggs', spider.project.name, '%s.egg' % version)

    def get_job_item_path(self, job):
        return os.path.join(self.data_dir, 'items', job.spider.project.name, '%s.jl' % job.id)

    def get_job_log_path(self, job):
        return os.path.join(self.data_dir, 'logs', job.spider.project.name, '%s.log' % job.id)


class StoragePathGeneratorV2():
    '''
    The new storage for multi-tenant support
    '''
    def __init__(self, data_dir):
        self.data_dir = data_dir

    def get_spider_egg_path(self,  spider, version):
        pass

    def get_job_item_path(self, data_dir, job):
        pass

    def get_job_log_path(self, data_dir, job):
        pass