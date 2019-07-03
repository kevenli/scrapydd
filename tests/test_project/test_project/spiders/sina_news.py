# -*- coding: utf-8 -*-
import scrapy
from ..items import PageItem

class SinaNewsSpider(scrapy.Spider):
    name = 'sina_news'
    allowed_domains = ['sina.com.cn']
    start_urls = ['http://news.sina.com.cn/']

    download_delay=3

    def parse(self, response):
        for url in [
            'https://mil.news.sina.com.cn/',
            'https://news.sina.com.cn/china/',
            'https://news.sina.com.cn/world/',
            'http://sports.sina.com.cn/nba/',
            'http://sports.sina.com.cn/g/premierleague/',
            'http://sports.sina.com.cn/csl/',
            'http://zhuanlan.sina.com.cn/',
            'http://blog.sina.com.cn/lm/history',
            'http://weather.sina.com.cn/'
        ]:
            yield scrapy.Request(url, callback=self.parse_page)

    def parse_page(self, response):
        title = response.xpath('//title/text()').extract_first()
        item = PageItem()
        item['url'] = response.url
        item['title'] = title
        yield item



