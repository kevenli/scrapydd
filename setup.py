from setuptools import setup, find_packages
import os
import scrapydd

version = scrapydd.__version__

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))
setup(
    name         = 'scrapydd',
    version      = version,
    author       ='kevenli',
    author_email ='pbleester@gmail.com',
    url          = 'http://github.com/kevenli/scrapydd',

    packages     = find_packages(),
    package_data = {
        'scrapydd': [
            'scrapydd.default.conf',
            'scrapyddagent.default.conf',
            'templates/*.html',
            'migrates/migrate.cfg',
        ],
    },
    entry_points = {
        'console_scripts': [
            'scrapydd = scrapydd.scripts.cmdline:main',
        ]
    },
    install_requires=[
        'tornado>=4.4.2',
        'apscheduler',
        'scrapyd',
        'sqlalchemy',
        'sqlalchemy-migrate',
    ]
)
