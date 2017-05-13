
# coding: utf-8

# In[ ]:

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import json
import urllib
import re
import function

client = MongoClient()
db = client['poll_charlie']
collect = db['ttv_news']

i = 0
crawled_new_links=[]
nocrawled_new_links=[]

#抓出所有文章連結
while True:
    i += 1
    res = requests.get('http://www.ttv.com.tw/news/catlist.asp?page=%d&Cat=A&NewsDay='%i)
    soup = BeautifulSoup(res.text)
    if i > 10 :
        break
    for entry in soup.select('.panel-body.padding-top-bottom-3 li a'):
        link = 'http://www.ttv.com.tw'+ entry['href']
        #分成已爬過的連結跟未爬過的連結 (以postgresql為主)
        if function.iscrawled(link):
            crawled_new_links.append(link)
        else:
            nocrawled_new_links.append(link)
        '''
        #分成已爬過的連結跟未爬過的連結 (以mongodb為主)
        if collect.find_one({'href':link}):
            crawled_new_links.append(link)
        else:
            nocrawled_new_links.append(link)
        '''
#抓未爬過的文章內容
ind1 = 1
t1 = len(nocrawled_new_links)
for link in nocrawled_new_links:
    res = requests.get(link)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text)
    try:
        date = soup.select('.ReportDate a')[0].text
        date = date+'+08'
    except Exception as e:
        date = None
        print 'error1',link
    try:
        title = soup.select('.title')[0].text.strip()
    except Exception as e:
        title = ''
        print 'error2',link
    try:
        content = ''.join(soup.select('.br')[0].text.split())
    except Exception as e:
        content = ''
        print 'error3',link
    try:
        href = link
    except Exception as e:
        print 'error5',link
    #抓新聞按讚數
    res = requests.get('http://api.facebook.com/restserver.php?method=links.getstats&format=json&urls=%s'%link)
    data = json.loads(res.text)[0]
    like_count = data['like_count']
    share_count = data['share_count']
    #提取文本關鍵字
    keywords = function.keyword_extract(content)
    #存進 mongodb
    doc = {'author':'台視新聞','date':date,'title':title,'content':content,'href':href,'share_count':share_count,'like_count':like_count,'keywords':keywords}
    collect.insert_one(doc)
    #存進 pgdb
    function.keywords_insert_pgdb(keywords)
    function.kw_relation_insert_pgdb(keywords)
    function.doc_insert_pgdb(doc,63,2) #doc,source,big_source
    function.doc_join_kw_insert_pgdb(keywords,href)
    function.daily_kw_insert_pgdb(keywords,date,63) #keywords,date,source_fk
    print '%d/%d'%(ind1,t1)
    ind1 += 1

ind2 = 1
t2 = len(crawled_new_links)
for link in crawled_new_links:
    try:
        res = requests.get(link)
    except Exception as e:
        print e
        continue
    soup = BeautifulSoup(res.text)
    href = link
    #更新已抓過的文章的按讚數
    res = requests.get('http://api.facebook.com/restserver.php?method=links.getstats&format=json&urls=%s'%link)
    data = json.loads(res.text)[0]
    like_count = data['like_count']
    share_count = data['share_count']
    #更新 doc in pgdb
    function.doc_update_pgdb(document_link=href,document_like=like_count,document_share=share_count)
    
    print '%d/%d'%(ind2,t2)
    ind2 += 1

print 'success'
print '新爬的文章數:',len(nocrawled_new_links)
print '更新的文章數:',len(crawled_new_links)
function.close_pgdb()

