import requests
from six.moves.urllib.parse import urljoin
import logging
from scrapy.utils.misc import load_object

logger = logging.getLogger(__name__)

class SSSServer:
    def __init__(self, base_url):
        self.base_url = base_url

    def request_seen(self, project, spider, request_fingerprint):
        path = '/s/%(project)s/%(spider)s/seen' % {'project': project, 'spider': spider}
        response = requests.request(method='POST', url= urljoin(self.base_url, path), data={'item': request_fingerprint})
        if response.status_code == 200:
            return int(response.content)
        else:
            logger.warning('Error when calling sss server dupefilter, response code is %d' % response.status_code)
            return 0

    def create_spider(self, project, spider):
        path = '/s/%(project)s/%(spider)s' % {'project': project, 'spider': spider}
        response = requests.request(method='PUT', url=urljoin(self.base_url, path))

    def update_spider_setting(self, project, spider, setting_key, setting_value):
        path = '/s/%(project)s/%(spider)s/settings/%(setting_key)s' % {'project': project, 'spider': spider,
                                                                       'setting_key': setting_key}
        response = requests.request(method='PUT', url=urljoin(self.base_url, path), data=str(setting_value))

def get_sss_from_settings(settings):
    sss_cls = settings.get('SSS_CLS', 'scrapydd.spiderlib.connection.SSSServer')
    if isinstance(sss_cls, str):
        sss_cls = load_object(sss_cls)
    sss_baseurl = settings.get('SSS_BASEURL')
    return sss_cls(sss_baseurl)