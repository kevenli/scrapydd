import logging

from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import request_fingerprint

from . import defaults
from connection import get_sss_from_settings


logger = logging.getLogger(__name__)

class SSSDupeFilter(BaseDupeFilter):
    '''SpiderStateService request duplicates filter.

    '''
    logger = logger

    def __init__(self, project, spider, server, settings, debug=False):
        self.project = project
        self.spider = spider
        self.server = server
        self.debug = debug
        self.settings = settings
        self.logdupes = True

    @classmethod
    def from_settings(cls, settings):
        logger.info('SSSDupeFilter from_settings')
        project = settings.get('BOT_NAME')
        spider = 'spider'
        server = get_sss_from_settings(settings)
        return cls(project, spider, server, settings)

    @classmethod
    def from_crawler(cls, crawler):
        logger.info('SSSDupeFilter from_crawler')
        return cls.from_spider(crawler.spider)

    @classmethod
    def from_spider(cls, spider):
        settings = spider.settings
        server = get_sss_from_settings(settings)
        project = settings.get('BOT_NAME')
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(project, spider.name, server, settings, debug=debug)

    def request_seen(self, request):
        """Returns True if request was already seen.

        Parameters
        ----------
        request : scrapy.http.Request

        Returns
        -------
        bool

        """
        fp = self.request_fingerprint(request)
        # This returns the number of values added, zero if already exists.
        added = self.server.request_seen(self.project, self.spider, fp)
        return added == 0

    def request_fingerprint(self, request):
        """Returns a fingerprint for a given request.

        Parameters
        ----------
        request : scrapy.http.Request

        Returns
        -------
        str

        """
        return request_fingerprint(request)

    def open(self):
        self.server.create_spider(self.project, self.spider)
        seen_expire = self.settings.get('SSS_SEEN_EXPIRE', defaults.SSS_SEEN_EXPIRE)
        self.server.update_spider_setting(self.project, self.spider, 'seen_expire', seen_expire)

    def close(self, reason=''):
        """Delete data on close. Called by Scrapy's scheduler.

        Parameters
        ----------
        reason : str, optional

        """
        self.clear()

    def clear(self):
        pass

    def log(self, request, spider):
        if self.debug:
            msg = "Filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request: %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False

        spider.crawler.stats.inc_value('dupefilter/filtered', spider=spider)