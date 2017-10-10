import unittest
import os
from scrapydd.models import init_database, session_scope, Project
from unittest.case import SkipTest
from scrapydd.config import Config

class ModelTest(unittest.TestCase):
    def setUp(self):
        if os.path.exists('test.db'):
            os.remove('test.db')
        self.config = Config(values={'database_url':'sqlite:///test.db'})

    def test_init(self):
        init_database(self.config)

    def test_init_ignore_file_already_exists(self):
        init_database(self.config)
        # an existing database should be version controlled, raised DatabaseAlreadyControlledError
        # the exception is ignorable
        init_database(self.config)


class SessionTest(unittest.TestCase):
    def setUp(self):
        if os.path.exists('test.db'):
            os.remove('test.db')
        self.config = Config(values={'database_url':'sqlite:///test.db'})
        init_database(self.config)

    def test_open(self):
        with session_scope() as session:
            pass

    def test_commit(self):
        with session_scope() as session:
            project = Project()
            project.name = 'test_project'
            session.add(project)

        # after scope closed, the data should be already commited

        with session_scope() as session:
            project = session.query(Project).first()

        self.assertEqual('test_project', project.name)

    def test_rollback(self):
        class CommitFailedError(Exception):
            pass
        with session_scope() as session:
            project = session.query(Project).first()
            self.assertIsNone(project)
        try:
            with session_scope() as session:
                project = Project()
                project.name = 'test_project'
                session.add(project)
                raise CommitFailedError()
        except CommitFailedError:
            pass


        # after an exception raised in the scope, the data should be rolled back.
        with session_scope() as session:
            project = session.query(Project).first()

        self.assertIsNone(project)