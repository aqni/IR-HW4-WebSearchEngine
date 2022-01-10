# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class nkeduItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field(default='')
    date = scrapy.Field()
    text = scrapy.Field()
    raw = scrapy.Field()
    links=scrapy.Field()
    
