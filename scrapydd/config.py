# -*- coding: utf-8 -*-
import ConfigParser
from cStringIO import StringIO
import os
from pkgutil import get_data
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError
import glob
from os.path import expanduser


class Config(object):
    """A ConfigParser wrapper to support defaults when calling instance
    methods, and also tied to a single section"""

    SECTION = 'server'

    def __init__(self, values=None, extra_sources=()):
        if values is None:
            sources = self._getsources()
            default_config = get_data(__package__, 'scrapydd.default.conf')
            default_config_stream = StringIO()
            default_config_stream.write(default_config)
            default_config_stream.seek(0, os.SEEK_SET)
            self.cp = SafeConfigParser()
            self.cp.readfp(default_config_stream)
            self.cp.read(self._getsources())
        else:
            self.cp = SafeConfigParser(values)
            self.cp.add_section(self.SECTION)

    def _load_config_file(self, fp):
        config = StringIO()
        config.write('[' + self.SECTION + ']\n')
        config.write(fp.read())
        config.seek(0, os.SEEK_SET)

        self.cp.readfp(config)

    def _getsources(self):
        sources = ['/etc/scrapydd/scrapydd.conf', r'c:\scrapydd\scrapydd.conf']
        sources += sorted(glob.glob('/etc/scrapyd/conf.d/*'))
        sources += ['scrapydd.conf']
        sources += [expanduser('~/.scrapydd.conf')]
        return sources

    def _getany(self, method, option, default):
        try:
            return method(self.SECTION, option)
        except (NoSectionError, NoOptionError):
            if default is not None:
                return default
            raise

    def get(self, option, default=None):
        return self._getany(self.cp.get, option, default)

    def getint(self, option, default=None):
        return self._getany(self.cp.getint, option, default)

    def getfloat(self, option, default=None):
        return self._getany(self.cp.getfloat, option, default)

    def getboolean(self, option, default=None):
        return self._getany(self.cp.getboolean, option, default)

    def items(self, section, default=None):
        try:
            return self.cp.items(section)
        except (NoSectionError, NoOptionError):
            if default is not None:
                return default
            raise

class AgentConfig(Config):
    SECTION = 'agent'