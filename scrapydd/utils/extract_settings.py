import sys
import os
import shutil
import tempfile
from contextlib import contextmanager
from scrapy.settings import overridden_settings, Settings, iter_default_settings
from scrapy.utils.project import get_project_settings
from scrapyd.eggutils import activate_egg
from scrapyd.eggstorage import FilesystemEggStorage
import scrapyd.config
import json

@contextmanager
def project_environment(project):
    eggstorage = FilesystemEggStorage(scrapyd.config.Config())
    version, eggfile = eggstorage.get(project)
    if eggfile:
        prefix = '%s-%s-' % (project, version)
        fd, eggpath = tempfile.mkstemp(prefix=prefix, suffix='.egg')
        lf = os.fdopen(fd, 'wb')
        shutil.copyfileobj(eggfile, lf)
        lf.close()
        activate_egg(eggpath)
    else:
        eggpath = None
    try:
        assert 'scrapy.conf' not in sys.modules, "Scrapy settings already loaded"
        yield
    finally:
        if eggpath:
            os.remove(eggpath)

def main():
    if len(sys.argv) > 0:
        project = sys.argv[1]
        os.environ['SCRAPY_PROJECT'] = project
    else:
        project = os.environ['SCRAPY_PROJECT']
    with project_environment(project):
        setting = get_project_settings()
        default_settings = dict(iter_default_settings())
        setting_dict = dict(overridden_settings(setting))
        for key in setting.attributes.keys():
            if key not in default_settings:
                setting_dict[key] = setting.attributes[key].value
        print json.dumps(setting_dict)


if __name__ == '__main__':
    main()