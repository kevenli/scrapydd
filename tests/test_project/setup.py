# Automatically created by: scrapyd-deploy

from setuptools import setup, find_packages

setup(
    name         = 'test_project',
    version      = '1.2',
    packages     = find_packages(),
    entry_points = {'scrapy': ['settings = test_project.settings']},
    install_requires = ['scrapy'],
)
