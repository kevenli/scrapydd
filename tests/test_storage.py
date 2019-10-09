from scrapydd.storage import ProjectStorage, ProjectStoragePathV1, ProjectStoragePathV2
from scrapydd.models import Project, Spider, HistoricalJob
from scrapydd.exceptions import EggFileNotFound
from unittest import TestCase
from uuid import uuid4
import os
from filecmp import cmp
from shutil import rmtree
from six import BytesIO



class ProjectStoragePathV1Test(TestCase):
    def test_get_project_eggs_dir(self):
        data_dir = '.'
        project = Project(id=1, name='test_project', storage_version=1)
        target = ProjectStoragePathV1(data_dir=data_dir)
        expect = './eggs/test_project'

        actual = target.get_project_eggs_dir(project)

        self.assertEqual(expect, actual)

    def test_get_job_item_path(self):
        data_dir = '.'
        project = Project(id=1, name='test_project', storage_version=1)
        spider = Spider(project=project, name='test_spider')
        job = HistoricalJob(id=uuid4().hex, spider=spider)
        target = ProjectStoragePathV1(data_dir=data_dir)

        actual = target.get_job_item_path(job)
        expect = './items/test_project/test_spider/%s.jl' % job.id
        self.assertEqual(expect, actual)

    def test_get_job_log_path(self):
        data_dir = '.'
        project = Project(id=1, name='test_project', storage_version=1)
        spider = Spider(project=project, name='test_spider')
        job = HistoricalJob(id=uuid4().hex, spider=spider)
        target = ProjectStoragePathV1(data_dir=data_dir)

        actual = target.get_job_log_path(job)
        expect = './logs/test_project/test_spider/%s.log' % job.id
        self.assertEqual(expect, actual)


class ProjectStoragePathV2Test(TestCase):
    def test_get_project_eggs_dir(self):
        data_dir = '.'
        project = Project(id=1, name='test_project', storage_version=2)
        target = ProjectStoragePathV2(data_dir=data_dir)
        expect = './projects/1/eggs'

        actual = target.get_project_eggs_dir(project)

        self.assertEqual(expect, actual)

    def test_get_job_item_path(self):
        data_dir = '.'
        project = Project(id=1, name='test_project', storage_version=1)
        spider = Spider(id=2, project=project, name='test_spider')
        job = HistoricalJob(id=uuid4().hex, spider=spider)
        target = ProjectStoragePathV2(data_dir=data_dir)

        actual = target.get_job_item_path(job)
        expect = './projects/1/spiders/test_spider/jobs/%s/items.jl' % job.id
        self.assertEqual(expect, actual)

    def test_get_job_log_path(self):
        data_dir = '.'
        project = Project(id=1, name='test_project', storage_version=1)
        spider = Spider(project=project, name='test_spider')
        job = HistoricalJob(id=uuid4().hex, spider=spider)
        target = ProjectStoragePathV2(data_dir=data_dir)

        actual = target.get_job_log_path(job)
        expect = './projects/1/spiders/test_spider/jobs/%s/job.log' % job.id
        self.assertEqual(expect, actual)

class ProjectStorageTest(TestCase):
    def test_put_egg(self):
        data_dir = 'data'
        project = Project(id=1, name='test_project', storage_version=1)
        test_project_egg = 'tests/test_project-1.0-py2.7.egg'
        fegg = open(test_project_egg, 'rb')
        version = '1.0'
        target = ProjectStorage(data_dir=data_dir, project=project)

        target.delete_egg(version)

        target.put_egg(fegg, version)

        target_egg_filepath = os.path.join(target.storage_provider.get_project_eggs_dir(project), '%s.egg' % version)

        self.assertTrue(os.path.exists(target_egg_filepath))
        self.assertTrue(cmp(test_project_egg, target_egg_filepath))

    def test_get_egg(self):
        data_dir = 'data'
        project = Project(id=1, name='test_project', storage_version=1)
        version = None
        target = ProjectStorage(data_dir=data_dir, project=project)
        self.test_put_egg()

        get_version, get_file = target.get_egg(version)
        self.assertEqual(open('tests/test_project-1.0-py2.7.egg', 'rb').read(), get_file.read())
        self.assertEqual('1.0', get_version)

    def test_get_egg_with_version(self):
        data_dir = 'data'
        project = Project(id=1, name='test_project', storage_version=1)
        version = '1.0'
        target = ProjectStorage(data_dir=data_dir, project=project)
        self.test_put_egg()

        get_version, get_file = target.get_egg(version)
        self.assertEqual(open('tests/test_project-1.0-py2.7.egg', 'rb').read(), get_file.read())
        self.assertEqual(version, get_version)

    def test_get_egg_with_none_exist_version(self):
        data_dir = 'data'
        project = Project(id=1, name='test_project', storage_version=1)
        version = '2.0'
        target = ProjectStorage(data_dir=data_dir, project=project)
        self.test_put_egg()

        try:
            get_version, get_file = target.get_egg(version)
            self.fail('Should not get non-existing version file')
        except EggFileNotFound:
            pass

    def test_get_egg_versions(self):
        data_dir = 'data'
        project = Project(id=1, name='test_project', storage_version=1)
        target = ProjectStorage(data_dir=data_dir, project=project)
        self.test_put_egg()

        self.assertEqual(target.list_egg_versions(), ['1.0'])

    def put_job_data(self):
        data_dir = 'data'
        project = Project(id=1, name='test_project', storage_version=1)
        spider = Spider(project=project, name='test_spider')
        job = HistoricalJob(id=uuid4().hex, spider=spider)

        log_stream = BytesIO(b'test log here')
        items_stream = BytesIO(b'{"foo": "bar}')
        target = ProjectStorage(data_dir=data_dir, project=project)
        target.put_job_data(job, log_stream, items_stream)

        saved_log_stream = target.get_job_log(job)
        log_stream.seek(0)
        self.assertEqual(log_stream.read(), saved_log_stream.read())

        saved_items_stream = target.get_job_items(job)
        items_stream.seek(0)
        self.assertEqual(items_stream.read(), saved_items_stream.read())

