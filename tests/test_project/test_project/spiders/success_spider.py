# -*- coding: utf-8 -*-
import scrapy
import time
from ..items import TestProjectItem


class SuccessSpiderSpider(scrapy.Spider):
    name = "success_spider"
    allowed_domains = ["baidu.com"]
    start_urls = (
        'http://www.baidu.com/',
    )

    def parse(self, response):
        time.sleep(10)
        item = TestProjectItem()
        item['name'] = 'test'
        yield item
