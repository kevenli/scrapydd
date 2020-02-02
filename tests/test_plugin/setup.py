from setuptools import setup, find_packages

setup(
    version='1.0',
    name='test_plugin',
    description='test_plugin',
    packages=find_packages(),
    entry_points={
        'scrapydd.spliderplugin': [
            'test_plugin = test_plugin.plugin:TestPlugin',
        ],
    },
    install_requires=[
        'scrapy',
        'scrapy-splitvariants'
    ],
)
