
from os import stat
from ESIndex import dbFilename, indexName
from elasticsearch import Elasticsearch
import sqlite3
from copy import deepcopy


class SearchQuery:
    WEIGHT = {
        'title': 10,
        'text': 10,
        'anchor': 10,
        'pagerank': 10,
    }
    tie_breaker = 0.3
    minimum_should_match = "20%"
    Personalize=1

    def __init__(self):
        # not in query
        self.from_ = 0
        self.size = 10
        # filter
        self.url = ''
        self.start_time = '1970-01-01'
        # query
        self.method = 'match'
        self.fields = ['title', 'text', 'anchor']
        self.query = ''
        self.xueyuan=''

    def getFilter(self):
        result = []
        if self.url:
            result.append({
                "prefix": {
                    "url": self.url
                }
            })
        if self.start_time:
            result.append({
                "range": {
                    "date": {
                        "gte": self.start_time
                    }
                }
            })
        return result

    def getMatch(self):
        wfields = [f"{field}^{self.WEIGHT[field]}" for field in self.fields]
        return [{
            "multi_match": {
                "query":                self.query,
                "type":                 "best_fields",
                "fields":               wfields,
                "tie_breaker":          self.tie_breaker,
                "minimum_should_match": self.minimum_should_match,
            }
        }]

    def getPageRank(self):
        return [{
            "rank_feature": {
                "field": "pagerank",
                "boost": self.WEIGHT['pagerank'],
                "log": {
                    "scaling_factor": 10
                },
            }
        }]

    def getPhrase(self):
        result = []
        for field in self.fields:
            result.append({
                "match_phrase": {
                    field: {
                        "query": self.query,
                        "boost": self.WEIGHT[field],
                    }
                }
            })
        return result

    def getWildcard(self):
        result = []
        for field in self.fields:
            result.append(
                {
                    "wildcard": {
                        field: {
                            "value": self.query,
                            "boost": self.WEIGHT[field],
                        }
                    },
                })
        return result
    
    def getPersonal(self):
        result=[]
        for field in self.fields:
            result.append({
                "match": {
                    field: {
                        "query": self.xueyuan,
                        "boost": self.Personalize,
                    }
                },
            })
        return result

    def getQuery(self):
        query = {
            "bool": {
                "must": [],
                "should": [],
                'filter': [],
            }
        }

        query['bool']["filter"] += self.getFilter()
        if self.method == "match":
            query['bool']['must'] += self.getMatch()
        elif self.method == "phrase":
            query['bool']['should'] += self.getPhrase()
        elif self.method == "wildcard":
            query['bool']['should'] += self.getWildcard()
        if self.xueyuan:
             query['bool']['should'] += self.getPersonal()

        query['bool']['must'] += self.getPageRank()
        print(query)

        return query


class WebSearch:
    TITLE_H_S = 20
    TEXT_H_S = 50

    def __init__(self, client: Elasticsearch, index_name: str, dbFilename: str):
        self.client = client
        self.index_name = index_name
        self.con = sqlite3.connect(dbFilename)

    def close(self):
        self.con.close()

    HIGHLIGHT = {
        "boundary_scanner_locale": "zh_CN",
        "pre_tags": ["<strong>"],
        "post_tags": ["</strong>"],
        "fields": {
            "title": {
                "fragment_size": TITLE_H_S,
                "no_match_size": TITLE_H_S,
                # "boundary_scanner":"word",
            },
            "text": {
                "fragment_size": TEXT_H_S,
                "no_match_size": TEXT_H_S,
                # "boundary_scanner":"sentence",
            },
            "anchor": {
                "fragment_size": TITLE_H_S,
                "no_match_size": TITLE_H_S,
                # "boundary_scanner":"sentence",
            }
        },
    }

    def seach(self, q: SearchQuery):
        res = self.client.search(index=self.index_name,
                                 query=q.getQuery(),
                                 highlight=WebSearch.HIGHLIGHT,
                                 from_=q.from_,
                                 size=q.size,
                                 )

        return {
            "pages": [{
                "url": raw['_source']['url'],
                "date":raw['_source']['date'],
                "title":raw['highlight']['title'],
                "text":raw['highlight']['text'],
                "anchor":raw['highlight']['anchor'] if 'anchor' in raw['highlight'] else '',
            }for raw in res['hits']['hits']],
        }

    def getSnapShot(self, url: str):
        pass

    def log(self, uid, q: SearchQuery):
        pass



if __name__ == "__main__":

    print(x.text)
    # es = Elasticsearch()
    # s = WebSearch(es, indexName, dbFilename)
    # q = SearchQuery()
    # q.query = "计算机学院官网"
    # res = s.seach(q)
    # print(res)
