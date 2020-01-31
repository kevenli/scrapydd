# -*- coding: utf-8 -*-
from six.moves import StringIO
from six import BytesIO
from six import ensure_str
import os
from pkgutil import get_data
from six.moves.configparser import ConfigParser, SafeConfigParser, NoSectionError, NoOptionError
import glob
from os.path import expanduser

def str2bool(v):
  return v.lower() in ("yes", "true", "t", "1", 'on')

class Config(object):
    """A ConfigParser wrapper to support defaults when calling instance
    methods, and also tied to a single section"""

    SECTION = 'server'
    _loaded_files = []

    def __init__(self, values=None, extra_sources=()):
        if values is None:
            sources = self._getsources()
            self.cp = ConfigParser()
            if __package__:
                default_config = ensure_str(get_data(__package__, 'scrapydd.default.conf'))
                self.cp.readfp(StringIO(default_config))
            for source in sources + list(extra_sources):
                if os.path.exists(source):
                    #self._load_config_file(open(source))
                    with open(source, 'r') as f:
                        self.cp.readfp(f)
        else:
            self.cp = ConfigParser(values)
            self.cp.add_section(self.SECTION)

    def _getsources(self):
        sources = ['/etc/scrapydd/scrapydd.conf', r'c:\scrapydd\scrapydd.conf']
        sources += sorted(glob.glob('/etc/scrapyd/conf.d/*'))
        sources += ['scrapydd.conf']
        sources += ['conf/scrapydd.conf']
        sources += [expanduser('~/.scrapydd.conf')]
        return sources

    def get(self, option, default=None):
        env_key = 'SCRAPYDD_' + option.replace('.', '_').upper()
        try:
            return os.getenv(env_key) or self.cp.get(self.SECTION, option)
        except (NoSectionError, NoOptionError):
            if default is not None:
                return default
            raise

    def _get(self, option, conv, default=None):
        return conv(self.get(option, default))

    def getint(self, option, default=None):
        return self._get(option, int, default)

    def getboolean(self, option, default=None):
        return self._get(option, str2bool, default)

    def getfloat(self, option, default=None):
        return self._get(option, float, default)


class AgentConfig(Config):
    SECTION = 'agent'
