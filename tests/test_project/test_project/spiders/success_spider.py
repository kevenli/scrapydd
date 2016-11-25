# -*- coding: utf-8 -*-
import scrapy


class SuccessSpiderSpider(scrapy.Spider):
    name = "success_spider"
    allowed_domains = ["baidu.com"]
    start_urls = (
        'http://www.baidu.com/',
    )

    def parse(self, response):
        pass
