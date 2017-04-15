# -*- coding: utf-8 -*-
import scrapy
import time
import logging

class LogSpiderSpider(scrapy.Spider):
    name = "log_spider"
    allowed_domains = ["baidu.com"]
    start_urls = (
        'http://www.baidu.com/',
    )

    def parse(self, response):
        logging.debug('debug message')
        logging.debug('info message')
        time.sleep(60)
