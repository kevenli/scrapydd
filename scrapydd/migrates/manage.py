#!/usr/bin/env python
import os
from migrate.versioning.shell import main

db_repository = os.path.join(os.path.dirname(__file__), '..', 'migrates')

if __name__ == '__main__':
    main(url='sqlite:///database.db', debug='False', repository=db_repository)
