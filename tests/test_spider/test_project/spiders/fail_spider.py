# -*- coding: utf-8 -*-
import scrapy
import sys

class FailSpiderSpider(scrapy.Spider):
    name = "fail_spider"
    allowed_domains = ["baidu.com"]
    start_urls = (
        'http://www.baidu.com/',
    )

    def parse(self, response):
        sys.exit(1)
