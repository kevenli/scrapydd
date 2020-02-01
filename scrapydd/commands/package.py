from scrapy.utils.python import retry_on_eintr
from scrapy.utils.conf import get_config, closest_scrapy_cfg
import os
import sys
import glob
from subprocess import check_call

_SETUP_PY_TEMPLATE = """
# Automatically created by: scrapydd
from setuptools import setup, find_packages
setup(
    name         = '%(project)s',
    version      = '1.0',
    packages     = find_packages(),
    entry_points = {'scrapy': ['settings = %(settings)s']},
    install_requires = [],
)
""".lstrip()

def _create_default_setup_py(**kwargs):
    with open('setup.py', 'w') as f:
        f.write(_SETUP_PY_TEMPLATE % kwargs)

def _build_egg():
    closest = closest_scrapy_cfg()
    os.chdir(os.path.dirname(closest))
    if not os.path.exists('setup.py'):
        scrapy_project_settings = get_config()
        settings = scrapy_project_settings.get('settings', 'default')
        project = scrapy_project_settings.get('deploy', 'project')
        _create_default_setup_py(settings=settings, project=project)
    d = 'dist'
    retry_on_eintr(check_call, [sys.executable, 'setup.py', 'clean', '-a', 'bdist_egg', '-d', d],
                   stdout=sys.stdout, stderr=sys.stderr)
    egg = glob.glob(os.path.join(d, '*.egg'))[0]
    return egg, d

class PackageCommand():
    def run(self):
        egg, d = _build_egg()
        print("Egg has been built: %s" % egg)
