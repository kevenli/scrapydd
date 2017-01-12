# -*- coding: utf-8 -*-
import scrapy
import time

class LogSpiderSpider(scrapy.Spider):
    name = "log_spider"
    allowed_domains = ["baidu.com"]
    start_urls = (
        'http://www.baidu.com/',
    )

    def parse(self, response):
        time.sleep(60)
