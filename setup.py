from setuptools import setup, find_packages
import os
import scrapydd
import io

version = scrapydd.__version__

def read_file(filename):
    with io.open(filename) as fp:
        return fp.read().strip()

def read_requirements(filename):
    return [line.strip() for line in read_file(filename).splitlines()
            if not line.startswith('#')]

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))
setup(
    name         = 'scrapydd',
    version      = version,
    author       ='kevenli',
    author_email ='pbleester@gmail.com',
    url          = 'http://github.com/kevenli/scrapydd',
    packages     = find_packages(exclude=('tests', 'tests.*')),
    package_data = {
        'scrapydd': [
            'scrapydd.default.conf',
            'scrapyddagent.default.conf',
            'templates/**/*.html',
            'templates/*.html',
            'migrates/migrate.cfg',
            'static/**/*',
        ],
    },
    entry_points = {
        'console_scripts': [
            'scrapydd = scrapydd.scripts.cmdline:main',
        ]
    },
    install_requires=read_requirements('requirements.txt'),
)
