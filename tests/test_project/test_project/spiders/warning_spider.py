# -*- coding: utf-8 -*-
import scrapy
import logging

class WarningSpiderSpider(scrapy.Spider):
    name = "warning_spider"
    allowed_domains = ["baidu.com"]
    start_urls = (
        'http://www.baidu.com/',
    )

    def parse(self, response):
        logging.warning('Warn test')
