
# coding: utf-8

# In[1]:

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import datetime
import json
import re
import function

client = MongoClient()
db = client['poll_charlie']
collect = db['storm_news']

nocrawled_new_links=[]
crawled_new_links=[]

links = ['http://www.storm.mg/category/118/%d'%i for i in range(1,6)]
#抓出所有文章連結
for entry1 in links:
    try:
        res = requests.get(entry1)
    except Exception as e:
        print e
        continue
    soup = BeautifulSoup(res.text)
    for entry2 in soup.select('.block-post'):
        link = 'http://www.storm.mg'+entry2.select('a')[-1]['href']
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
print '抓新文章中...'
#抓文章內容
ind1 = 1
t1 = len(nocrawled_new_links)
for link in nocrawled_new_links:
    try:
        res = requests.get(link)
    except Exception as e:
        print e
        continue
    soup = BeautifulSoup(res.text)
    try:
        date = datetime.datetime.strptime(soup.select('.date')[0].text.encode('utf8'),'%Y年%m月%d日 %H:%M')
        date -= datetime.timedelta(hours = 8)
    except Exception as e:
        date = None
        print 'error1',link
    try:
        title = soup.select('.title')[0].text.strip()
    except Exception as e:
        title = ''
        print 'error2',link
    try:
        content = ''
        for j in soup.select('article p')[:-2]:
            content += j.text.strip()
    except Exception as e:
        content = ''
        print 'error3',link
    try:
        href = link
    except Exception as e:
        href = ''
        print 'error4',link
    #抓文章按讚數.分享數
    res = requests.get('http://api.facebook.com/restserver.php?method=links.getstats&format=json&urls=%s'%link)
    data = json.loads(res.text)[0]
    like_count = data['like_count']
    share_count = data['share_count']
    comment_count = data['comment_count']
    #提取文本關鍵字
    keywords = []
    for i in soup.select('.keywords a'):
        keywords.append(i.text)
    #存進 mongodb
    doc = {'author':'風傳媒','date':date,'title':title,'content':content,'href':href,'share_count':share_count,'like_count':like_count,'comment_count':comment_count,'keywords':keywords}
    collect.insert_one(doc)
    #存進 pgdb
    function.keywords_insert_pgdb(keywords)
    function.kw_relation_insert_pgdb(keywords)
    function.doc_insert_pgdb(doc,48,2) #doc,source,big_source
    function.doc_join_kw_insert_pgdb(keywords,href)
    function.daily_kw_insert_pgdb(keywords,date,48) #keywords,date,source_fk
    print '%d/%d'%(ind1,t1)
    ind1 += 1

ind2 = 1
t2 = len(crawled_new_links)
print '更新中...'
for link in crawled_new_links:
    #更新已抓過的文章的按讚數
    res = requests.get('http://api.facebook.com/restserver.php?method=links.getstats&format=json&urls=%s'%link)
    data = json.loads(res.text)[0]
    like_count = data['like_count']
    share_count = data['share_count']
    comment_count = data['comment_count']
    #更新 mongodb
    collect.update_one({'href':link},{'$set':{'share_count':share_count,'like_count':like_count,'comment_count':comment_count}})
    #更新 doc in pgdb
    function.doc_update_pgdb(document_link=link,document_like=like_count,document_share=share_count,comment_count=comment_count)

    print '%d/%d'%(ind2,t2)
    ind2 += 1
    
print 'success'
print '新爬的文章數:',len(nocrawled_new_links)
print '更新的文章數:',len(crawled_new_links)
function.close_pgdb()

