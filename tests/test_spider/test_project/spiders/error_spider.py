# -*- coding: utf-8 -*-
import scrapy


class ErrorSpiderSpider(scrapy.Spider):
    name = "error_spider"
    allowed_domains = ["baidu.com"]
    start_urls = (
        'http://www.baidu.com/',
    )

    def parse(self, response):
        raise Exception('Error')
