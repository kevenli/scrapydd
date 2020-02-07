import sys
import os
import pkg_resources
import subprocess


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
            ret = p.wait(timeout=60)
            return ret
        except subprocess.TimeoutExpired:
            sys.stderr.write('pip install process timeout:\n')
            return 1
    return 0


def main(argv=None):
    from scrapy.cmdline import execute
    from scrapy.settings import Settings
    settings = None
    egg_path = os.environ.get('SCRAPY_EGG')

    if egg_path:
        distribute = activate_egg(egg_path)
        ret = install_requirements(distribute)
        if ret > 0:
            sys.exit(ret)

    settings_module = os.environ.get('SCRAPY_SETTINGS_MODULE')
    if settings_module:
        settings = Settings()
        settings.setmodule(settings_module, priority='project')

        extra_settings_module = os.environ.pop('SCRAPY_EXTRA_SETTINGS_MODULE',
                                               None)
        if extra_settings_module:
            settings.setmodule(extra_settings_module, priority='project')

    execute(argv=argv, settings=settings)

if __name__ == '__main__':
    main()
