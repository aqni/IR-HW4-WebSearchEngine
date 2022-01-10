# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import sqlite3
from bs4 import BeautifulSoup

class NKEduPipeline(object):
    def open_spider(self, spider):
        self.con = sqlite3.connect('data.sqlite')
        self.cu = self.con.cursor()

    def close_spider(self, spider):
        self.con.commit()
        self.con.close() 
    
    sql_insert_page='INSERT OR REPLACE INTO webpage (wp_id,wp_title,wp_date,wp_text,wp_raw) VALUES (?,?,?,?,?)'
    sql_insert_url='INSERT OR IGNORE INTO url(u_url) VALUES (?)'
    sql_select_uid='SELECT u_id FROM url WHERE u_url==?'
    sql_insert_link='INSERT OR REPLACE INTO link (suid,duid,anchor) SELECT ? ,u_id,? FROM url WHERE u_url==?'
    
    def process_item(self, item, spider):
        self.cu.execute(NKEduPipeline.sql_insert_url,(item['url'],))
        self.cu.execute(NKEduPipeline.sql_select_uid,(item['url'],))
        suid = self.cu.fetchone()[0]
        row=(suid,item['title'],item['date'],item['text'],item['raw'],)
        self.cu.execute(NKEduPipeline.sql_insert_page,row)
        urls=[]
        linkanchors=[]
        for link in item['links']:
            urls.append((link.url,))
            anchor=BeautifulSoup(link.text,'lxml').get_text(" ", strip=True)
            linkanchors.append((suid,anchor,link.url,))
        self.cu.executemany(NKEduPipeline.sql_insert_url,urls)
        self.cu.executemany(NKEduPipeline.sql_insert_link,linkanchors)
        # return item




