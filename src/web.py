from flask import Flask
from flask import render_template
from flask import request
from ESIndex import dbFilename,indexName
from elasticsearch import Elasticsearch
from Search import *
import urllib
import requests
import json

app = Flask(__name__)
es = Elasticsearch()
s= WebSearch(es,indexName,dbFilename)

users={"计算机":{"password":"jsj","xueyuan":"计算机"}}
history={"计算机":[]}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search',methods=['GET'])
def search():
    q=SearchQuery()
    q.query= request.values.get('query',default='',type=str)
    q.start_time=request.values.get('date',default='1970-01-01',type=str)
    q.fields=request.values.getlist('fields',type=str)
    if not q.fields: q.fields=['title','text','anchor']
    q.from_=request.values.get('from',default=0,type=int)
    q.size=request.values.get('size',default=10,type=int)
    q.method=request.values.get('method',default='match',type=str)
    q.url=request.values.get('url',default='http',type=str)
    username=request.values.get('username')
    h=None
    if username and username in users:
        if q!='':history[username].insert(0,q.query)
        h=history[username][:6]
        q.xueyuan=users[username]['xueyuan']
    res=s.seach(q)
    json.dump(users, open("users.json", 'w'))
    json.dump(history, open("history.json", 'w'))
    return render_template('result.html',pages=res['pages'],query=q.query,user=username,history=h)

def highligth(query,html):
    url='http://localhost:9200/_analyze'
    data={
        "analyzer": "ik_smart",
        "text": query
    }
    x = requests.post(url, json=data)
    j=json.loads(x.text)
    keywords=[token['token'] for token in j["tokens"]]
    for kw in keywords:
        old=kw
        new='<font style="background: #ff9632">'+kw+'</font>'
        html=html.replace(kw,'<font style="background: #ff9632">'+kw+'</font>')
    return html


PAGE_SQL="SELECT wp_raw FROM webpage WHERE wp_id ==(SELECT u_id FROM url WHERE u_url==?)"
@app.route('/snapshot',methods=['GET'])
def snapshot():
    url=request.values.get('url')
    query=request.values.get('query')

    db=sqlite3.connect(dbFilename)
    if url:
        result=db.execute(PAGE_SQL,(url,)).fetchone()[0]
        result.replace('\r\n','')
        h=highligth(query,result)
        return h
    return ''

@app.route('/login',methods=['GET'])
def login():
    username=request.values.get('username')
    password=request.values.get('password')
    if username not in users: return "无此用户"
    if password != users[username]['password']:return "密码错误"

    return render_template('result.html',pages=[],query='',user=username,history=history[username][:6])

@app.route('/signup',methods=['GET'])
def signup():
    username=request.values.get('username')
    password=request.values.get('password')
    xueyuan=request.values.get('xueyuan')
    if username in users: return "用户已存在"
    users[username]={
        "password":password,
        "xueyuan":xueyuan,
    }
    history[username]=[]
    return render_template('result.html',pages=[],query='',user=username,history=[])

if __name__ == '__main__':
    history=json.load(open("history.json"))
    users=json.load(open("users.json"))
    app.run()

#查询时加权https://www.elastic.co/guide/cn/elasticsearch/guide/current/query-time-boosting.html
#rank_feature https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-rank-feature-query.html
#短语查询 match_phrase
#支持分页 https://www.cnblogs.com/hirampeng/p/10035858.html