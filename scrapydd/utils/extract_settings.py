import sys
import os
from scrapy.settings import overridden_settings, Settings, iter_default_settings
from scrapy.utils.project import get_project_settings
from .runner import FilesystemEggStorage, activate_egg, project_environment
import json

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