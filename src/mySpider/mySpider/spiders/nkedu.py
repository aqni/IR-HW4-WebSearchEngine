import scrapy
from scrapy.linkextractors import LinkExtractor
from mySpider.items import nkeduItem
from bs4 import BeautifulSoup
import time
from sqlite3.dbapi2 import DatabaseError

class NkeduSpider(scrapy.Spider):
    name = 'nkedu'
    allowed_domains = ['nankai.edu.cn']
    start_urls = ['https://www.nankai.edu.cn']

    link_extractor=LinkExtractor(allow_domains=allowed_domains)
    def parse(self, response):
        links = self.link_extractor.extract_links(response)
        
        item=nkeduItem(
            url=response.url,
            title=response.xpath('//title/text()').get(''),
            date= time.time(),
            text=BeautifulSoup(response.text,'lxml').get_text(" ", strip=True),
            raw=response.text,
            links=links,
        )
        yield item
        yield from response.follow_all(links, callback=self.parse)


# scrapy-splash 处理动态页面
# 结果保存在sqlite中
# https://blog.csdn.net/Ypersistence/article/details/53728785
# pagerank igraph >10 networkx  
# in sqlite:
# id url pagerankscore 
# sid did anchortext
# url title date text snapshop 
# es支持Rank Feature
# Elasticsearch中使用painless实现评分

