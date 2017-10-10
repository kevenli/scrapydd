import unittest
import os
from scrapydd.models import init_database, session_scope, Project
from unittest.case import SkipTest

class ModelTest(unittest.TestCase):
    def setUp(self):
        self.skipTest('do not test database recreation')
        if os.path.exists('database.db'):
            os.remove('database.db')

    def test_init(self):
        init_database()

    def test_init_ignore_file_already_exists(self):
        init_database()
        # an existing database should be version controlled, raised DatabaseAlreadyControlledError
        # the exception is ignorable
        init_database()


class SessionTest(unittest.TestCase):
    def setUp(self):
        if os.path.exists('database.db'):
            os.remove('database.db')
        init_database()

    def test_open(self):
        with session_scope() as session:
            pass

    def test_commit(self):
        with session_scope() as session:
            project = Project()
            project.name = 'test project'
            session.add(project)

        # after scope closed, the data should be already commited

        with session_scope() as session:
            project = session.query(Project).first()

        self.assertEqual('test project', project.name)



    def test_rollback(self):
        class CommitFailedError(Exception):
            pass
        try:
            with session_scope() as session:
                project = Project()
                project.name = 'test project'
                session.add(project)
                raise CommitFailedError()
        except CommitFailedError:
            pass


        # after an exception raised in the scope, the data should be rolled back.
        with session_scope() as session:
            project = session.query(Project).first()

        self.assertIsNone(project)