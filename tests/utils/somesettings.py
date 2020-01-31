# an extra spider settings module should import the original settings
# to global and then overwrite variables
from test_project.settings import *

# a single directive to overwrite a variable
SOME_SETTING = 'TestModule'

# to manipulate dictionaries, check whether it is defined first.
if not 'SPIDER_MIDDLEWARES' in globals():
    SPIDER_MIDDLEWARES = {}
SPIDER_MIDDLEWARES['scrapy.spidermiddlewares.depth.DepthMiddleware'] = 300
