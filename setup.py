from setuptools import setup, find_packages
import os

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))
setup(
    name         = 'scrapymill',
    version      = '0.1',
    packages     = find_packages(),
    entry_points = {'console_scripts': [
    'scrapydd = scrapydd.scripts.scrapydd_run:main',
    'scrapyddagent = scrapydd.scripts.scrapyddagent:main',
    ]}
)
