
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
collect = db['nexttv_news']
i = 0
crawled_new_links=[]
nocrawled_new_links=[]

#抓出所有文章連結
res = requests.get('http://www.nexttv.com.tw/nexttv_ajax/getdata?m=getPostsByCate&category=n-p-realtime-politics&order=date&limit=100')
data = json.loads(res.text)['data']
for entry in json.loads(res.text)['data']:
    link = 'http://www.nexttv.com.tw/news/realtime/politics/'+ entry['post_id']
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
        date = soup.select('.date')[0].text
        date = date+'+08'
    except Exception as e:
        date = None
        print 'error1',link
    try:
        title = soup.select('.hd h1')[0].text
    except Exception as e:
        title = ''
        print 'error2',link
    try:
        content = soup.select('.content p')[0].text
    except Exception as e:
        content = ''
        print '沒有內文',link
    try:
        href = link
    except Exception as e:
        print 'error5',link
    #抓新聞按讚數
    res = requests.get('http://api.facebook.com/restserver.php?method=links.getstats&format=json&urls=%s'%link)
    data = json.loads(res.text)[0]
    like_count = data['like_count']
    share_count = data['share_count']
    #抓新聞的 comments
    try:
        comments = function.get_fb_comments(data['comments_fbid'])
    except Exception as e:
        comments = []
        print '抓取fb_comments錯誤',e
    #提取文本關鍵字
    keywords = function.keyword_extract(content)
    #存進 mongodb
    doc = {'author':'壹電視新聞','date':date,'title':title,'content':content,'href':href,'share_count':share_count,'like_count':like_count,'comments':comments,'keywords':keywords}
    collect.insert_one(doc)
    #存進 pgdb
    function.keywords_insert_pgdb(keywords)
    function.kw_relation_insert_pgdb(keywords)
    function.doc_insert_pgdb(doc,68,2) #doc,source,big_source
    function.doc_join_kw_insert_pgdb(keywords,href)
    function.daily_kw_insert_pgdb(keywords,date,68) #keywords,date,source_fk
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
    #更新已抓過的文章的 comments
    try:
        comments = function.get_fb_comments(data['comments_fbid'])
    except Exception as e:
        print e
        comments = []
    #不讓空的 comments蓋掉舊的資料
    if comments == []:
        collect.update_one({'href':href},{'$set':{'share_count':share_count,'like_count':like_count}})
    else:
        collect.update_one({'href':href},{'$set':{'share_count':share_count,'like_count':like_count,'comments':comments}})
    #更新 doc in pgdb
    function.doc_update_pgdb(document_link=href,document_like=like_count,document_share=share_count)
    
    print '%d/%d'%(ind2,t2)
    ind2 += 1

print 'success'
print '新爬的文章數:',len(nocrawled_new_links)
print '更新的文章數:',len(crawled_new_links)
function.close_pgdb()

