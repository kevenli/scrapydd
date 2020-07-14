import sys
import os
import pkg_resources
import subprocess
import logging
from scrapy.utils.project import get_project_settings
from scrapy.settings import Settings

LOGGER = logging.getLogger(__name__)


def activate_egg(eggpath):
    """Activate a Scrapy egg file. This is meant to be used from egg runners
    to activate a Scrapy egg file. Don't use it from other code as it may
    leave unwanted side effects.
    """
    try:
        d = next(pkg_resources.find_distributions(eggpath))
    except StopIteration:
        raise ValueError("Unknown or corrupt egg")
    d.activate()
    settings_module = d.get_entry_info('scrapy', 'settings').module_name
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_module)
    return d


def activate_project(egg_path: str = None) -> Settings:
    """
    Activate a scrapy project environment.
    This will load settings from current environment ( specified project egg or
    inside a project folder), instantiate the Settings, install required package
    of the project.
    :param egg_path: A project package egg.
    :return: Settings : Settings
    """
    if egg_path:
        LOGGER.debug('activating egg %s', egg_path)
        distribute = activate_egg(egg_path)
        ret = install_requirements(distribute)
        if ret > 0:
            sys.exit(ret)
        settings_module = os.environ.get('SCRAPY_SETTINGS_MODULE')
        if settings_module:
            settings = Settings()
            settings.setmodule(settings_module, priority='project')
    else:
        settings = get_project_settings()
    return settings


def install_requirements(distribute, append_log=False):
    requires = [str(x) for x in distribute.requires()]
    if requires:
        env = os.environ.copy()
        # python -W ignore: ignore the python2 deprecate warning.
        # pip --disable-pip-version-check: ignore pip version warning.
        pargs = [sys.executable, '-W', 'ignore', '-m', 'pip',
                 '--disable-pip-version-check',
                 'install']
        pargs += requires
        stdout = subprocess.PIPE
        if append_log:
            stdout = open('pip.log', 'w')
        p = subprocess.Popen(pargs, stdout=stdout, stderr=subprocess.PIPE,
                             env=env)
        try:
            stdout, stderr = p.communicate(timeout=600)
            # ret = p.returncode
            return 0
        except subprocess.TimeoutExpired:
            sys.stderr.write('pip install process timeout:\n')
            return 1
    return 0


def execute(argv=None, settings=None):
    from scrapy.cmdline import execute as scrapy_execute
    scrapy_execute(argv=argv, settings=settings)


def main(argv=None):
    egg_path = os.environ.pop('SCRAPY_EGG', None)

    settings = activate_project(egg_path=egg_path)

    # TODO: replace the text-file-based SCRAPY_EXTRA_SETTINGS_MODULE with the
    #  mechanism consist of `activate_project` and `execute` methods, to
    #  provide a more robust and direct api to execute scrapy process.
    #  `activate_project` method activate the project and return a Settings
    #  instance , `execute` accept a Settings instance as the parameter.
    #  Caller can manipulate Settings arbitrarily before `execute`.
    extra_settings_module = os.environ.pop('SCRAPY_EXTRA_SETTINGS_MODULE',
                                           None)
    if extra_settings_module:
        LOGGER.info('using SCRAPY_EXTRA_SETTINGS_MODULE: %s',
                    extra_settings_module)
        settings.setmodule(extra_settings_module, priority='project')

    execute(argv=argv, settings=settings)


if __name__ == '__main__':
    main()
