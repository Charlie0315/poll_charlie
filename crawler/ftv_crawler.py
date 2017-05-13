
# coding: utf-8

# In[ ]:

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import datetime
import json
import urllib
import re
import function

client = MongoClient()
db = client['poll_charlie']
collect = db['ftv_news']
i = 0
crawled_new_links=[]
nocrawled_new_links=[]

#抓出所有文章連結
res = requests.get('http://news.ftv.com.tw/NewsList.aspx?Class=P')
soup = BeautifulSoup(res.text)
for entry in soup.select('.h2'):
    link = 'http://news.ftv.com.tw/'+ entry['href']
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
    #res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text)
    try:
        date = soup.select('.ndate')[0].text.replace('/','-')
        date = date+'+08'
    except Exception as e:
        date = None
        print 'error1',link
    try:
        title = soup.select('#h1')[0].text
    except Exception as e:
        title = ''
        print 'error2',link
    try:
        content = soup.select('#newscontent')[0].text.strip()
    except Exception as e:
        content = ''
        print '沒有內文',link
    try:
        href = link
    except Exception as e:
        print 'error5',link
    #提取文本關鍵字
    keywords = function.keyword_extract(content)
    #存進 mongodb
    doc = {'author':'民視新聞','date':date,'title':title,'content':content,'href':href,'keywords':keywords}
    collect.insert_one(doc)
    #存進 pgdb
    function.keywords_insert_pgdb(keywords)
    function.kw_relation_insert_pgdb(keywords)
    function.doc_insert_pgdb(doc,69,2) #doc,source,big_source
    function.doc_join_kw_insert_pgdb(keywords,href)
    function.daily_kw_insert_pgdb(keywords,date,69) #keywords,date,source_fk
    print '%d/%d'%(ind1,t1)
    ind1 += 1

print 'success'
print '新爬的文章數:',len(nocrawled_new_links)
print '更新的文章數:',len(crawled_new_links)
function.close_pgdb()

