from setuptools import setup, find_packages
import os
import scrapydd

version = scrapydd.__version__

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))
setup(
    name         = 'scrapydd',
    version      = version,
    packages     = find_packages(),
    package_data = {
        'scrapydd': [
            'scrapydd.default.conf',
            'scrapyddagent.default.conf',
            'templates/*.html'
        ],
        'migrates': [
            'migrate.cfg',
        ]
    },
    entry_points = {
        'console_scripts': [
            'scrapydd = scrapydd.scripts.scrapydd_run:main',
            'scrapyddagent = scrapydd.scripts.scrapyddagent:main',
        ]
    },
)
