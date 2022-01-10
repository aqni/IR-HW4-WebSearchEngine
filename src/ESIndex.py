from typing import Iterable
from tqdm import tqdm
import sqlite3
from elasticsearch import Elasticsearch, helpers
from datetime import datetime

class PageIndex:
    mappings = {
        "properties": {
            "url": {"type": "keyword",},
            "date": {"type": "date", "format": "strict_date_optional_time",},
            "title":{"type": "text","analyzer": "ik_max_word",},
            "text":{"type": "text","analyzer":"ik_smart",},
            "pagerank":{"type": "rank_feature",},
            "anchor":{"type": "text","analyzer": "ik_max_word",},
        }
    }

    def __init__(self,client:Elasticsearch,index_name:str):
        self.client=client
        self.indexName=index_name
    
    def reCreateIndex(self):
        if self.client.indices.exists(index=self.indexName):
            print(self.client.indices.delete(index=self.indexName))
        print(self.client.indices.create(index=self.indexName, mappings=PageIndex.mappings))
    
    def indexDocs(self,batch:Iterable):
        helpers.bulk(self.client,batch,index=self.indexName)
    

class FetchPage:
    def __init__(self,dbFilename:str):
        self.dbFilename=dbFilename

    #先在duid建立索引
    #清理anchor中的链接 UPDATE link SET anchor ='' WHERE anchor LIKE 'http%'
    
    P_SQL="""
    SELECT u_url, wp_date, wp_title, wp_text, score,
        (SELECT GROUP_CONCAT(DISTINCT anchor) FROM link WHERE duid == u_id) as anchors
    FROM url INNER JOIN webpage ON u_id == wp_id
    """
    def __enter__(self):
        self.con=sqlite3.connect(self.dbFilename)
        print("executing sql...")
        self.cu = self.con.execute(FetchPage.P_SQL)
        return self
 
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()
    
    FETCH_BATCH=1000
    def fetchOnlyOnce(self):
        while True:
            res=self.cu.fetchmany(FetchPage.FETCH_BATCH)
            if not res: break
            for url,ts,title,text,score,anchor in res:
                yield {
                    "url": url,
                    "date": datetime.fromtimestamp(ts).isoformat(),
                    "title":title,
                    "text":text,
                    "pagerank":score if score else 0.0000001,
                    "anchor":anchor,
                }

indexName = 'page_map'
dbFilename="src/mySpider/data.sqlite"

if __name__ == '__main__':
    es = Elasticsearch()
    pi=PageIndex(es,indexName)
    pi.reCreateIndex()
    batch=[]
    with FetchPage(dbFilename) as f:
        for action in tqdm(f.fetchOnlyOnce(),desc="indexing"):
            batch.append(action)
            if len(batch)>1000: 
                pi.indexDocs(batch)
                batch.clear()
        pi.indexDocs(batch)
    print("indexed.")
