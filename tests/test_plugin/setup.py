from setuptools import setup, find_packages

setup(
    version='1.0',
    name='test_plugin',
    description='test_plugin',
    packages=find_packages(),
    entry_points={
        'scrapydd.plugin.execute': [
            'test_plugin = test_plugin.plugin:execute',
        ],
        'scrapydd.plugin.desc': [
            'test_plugin = test_plugin.plugin:desc',
        ]
    },
    install_requires=[
        'scrapy',
        'scrapy-splitvariants'
    ],
)
