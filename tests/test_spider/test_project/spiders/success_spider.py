# -*- coding: utf-8 -*-
import scrapy
import time
from ..items import PageItem


class SuccessSpiderSpider(scrapy.Spider):
    name = "success_spider"
    allowed_domains = ["baidu.com"]
    start_urls = (
        'http://www.baidu.com/',
    )

    def parse(self, response):
        item = PageItem()
        item['url'] = response.url
        item['title'] = response.xpath('/html/head/title/text()').extract_first()
        yield item
