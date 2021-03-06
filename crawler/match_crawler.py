
# coding: utf-8

# In[1]:

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import json
import urllib
import function

client = MongoClient()
db = client['poll_charlie']
collect = db['match_news']

i = 0
crawled_new_links=[]
nocrawled_new_links=[]

#抓出所有文章連結
while True:
    i+=1
    if i == 20:
        break
    res = requests.get('http://match.net.tw/pc/news/list/%d/102'%(i))
    soup = BeautifulSoup(res.text)
    #該頁沒有新聞
    if soup.select('.single-con1 li a') == []:
        break
    for entry in soup.select('.single-con1 li a'):
        link = 'http://match.net.tw'+ entry['href']
        #分成已爬過的連結跟未爬過的連結
        if function.iscrawled(link):
            crawled_new_links.append(link)
        else:
            nocrawled_new_links.append(link)

#抓未爬過的文章內容
ind1 = 1
t1 = len(nocrawled_new_links)
for link in nocrawled_new_links:
    res = requests.get(link)
    soup = BeautifulSoup(res.text)
    try:
        date = soup.select('.date')[0].text.split(u'\xa0\xa0')[1]
        date = date.replace('/','-')
        date = date+'+08'
        print date
    except Exception as e:
        date = None
        print 'error1',link
    try:
        title = soup.select('.conmax h3')[0].text
    except Exception as e:
        title = ''
        print 'error2',link
    try:
        content = soup.select('.txt')[0].text.strip().split('\n')[0]
    except Exception as e:
        content = ''
        print 'error3',link
    try:
        href = link
    except Exception as e:
        href = ''
        print 'error4',link
    #提取文本關鍵字
    keywords = function.keyword_extract(content)
    #存進 mongodb
    doc = {'date':date,'title':title,'content':content,'href':href,'keywords':keywords}
    collect.insert_one(doc)
    #存進 pgdb
    function.keywords_insert_pgdb(keywords)
    #function.kw_relation_insert_pgdb(keywords)
    #function.doc_insert_pgdb(doc,40,2) #doc,source,big_source
    #function.doc_join_kw_insert_pgdb(keywords,href)
    #function.daily_kw_insert_pgdb(keywords,date,40) #keywords,date,source_fk
    
    print '%d/%d'%(ind1,t1)
    ind1 += 1
    
print 'success'
print '新爬的文章數:',len(nocrawled_new_links)
print '更新的文章數:',len(crawled_new_links)
function.close_pgdb()

