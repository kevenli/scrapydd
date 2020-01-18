import sys
import shutil
import tempfile
import os
import pkg_resources
from contextlib import contextmanager
from subprocess import Popen, PIPE
from six import ensure_binary
from six import next
from ..eggstorage import FilesystemEggStorage

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


@contextmanager
def project_environment(project):
    eggstorage = FilesystemEggStorage({})
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


def install_requirements(distribute, append_log=False):
    requires = [str(x) for x in distribute.requires()]
    if requires:
        pargs = [sys.executable, '-W', 'ignore', '-m', 'pip', '--disable-pip-version-check',
                 'install']
        pargs += requires
        stdout = PIPE
        if append_log:
            stdout = open('pip.log', 'wb')
            stdout.write(ensure_binary(' '.join(pargs) + '\n'))
        p = Popen(pargs, stdout=stdout, stderr=sys.stderr)
        ret = p.wait()
        return ret
    return 0


def main(argv=None):
    from scrapy.cmdline import execute
    from scrapy.settings import Settings
    settings = Settings()
    egg_path = os.environ.get('SCRAPY_EGG')
    if egg_path:
        distribute = activate_egg(egg_path)
        ret = install_requirements(distribute)
        if ret > 0:
            sys.exit(ret)
        settings_module = os.environ.get('SCRAPY_SETTINGS_MODULE')
        if settings_module:
            settings.setmodule(settings_module, priority='project')
        extra_settings_module = os.environ.get('SCRAPY_EXTRA_SETTINGS_MODULE')
        if extra_settings_module:
            settings.setmodule(extra_settings_module)
        return execute(argv=argv, settings=settings)
    project = os.environ['SCRAPY_PROJECT']
    with project_environment(project):
        execute()


if __name__ == '__main__':
    main()
