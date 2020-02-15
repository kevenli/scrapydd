from setuptools import setup, find_packages

setup(
    version='1.0',
    name='scrapy_splitvariants_plugin',
    description='scrapy_splitvariants_plugin',
    packages=find_packages(exclude=['tests', 'tests.*']),
    entry_points={
        'scrapydd.spliderplugin': [
            'splitvariants = scrapy_splitvariants_plugin.plugin:Plugin',
        ],
    },
    install_requires=[
        'scrapy',
        'scrapy-splitvariants'
    ],
)
