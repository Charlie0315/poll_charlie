
# coding: utf-8

# In[1]:

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
collect = db['apple_news']

i = 0
crawled_new_links=[]
nocrawled_new_links=[]

#抓出所有文章連結
while True:
    i += 1
    res = requests.get('http://www.appledaily.com.tw/realtimenews/section/politics/'+str(i))
    soup = BeautifulSoup(res.text)
    #該頁沒有新聞
    if soup.select('.rtddt') == []:
        break
    for entry in soup.select('.rtddt'):
        link = 'http://www.appledaily.com.tw'+ entry.select('a')[0]['href']
        #去除網址最後中文部分
        a = link.split('/')
        link = a[0]+'//'+a[2]+'/'+a[3]+'/'+a[4]+'/'+a[5]+'/'+a[6]+'/'+a[7]
        #分成已爬過的連結跟未爬過的連結 (以postgresql為主)
        '''
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
#抓未爬過的文章內容
ind1 = 1
t1 = len(nocrawled_new_links)
for link in nocrawled_new_links:
    res = requests.get(link)
    soup = BeautifulSoup(res.text)
    try:
        date = soup.select('time')[0].text
        date = date.replace(u'年','-')
        date = date.replace(u'月','-')
        date = date.replace(u'日',' ')
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
        content = soup.select('#summary')[0].text
    except Exception as e:
        content = ''
        print 'error3',link
    try:
        popular = soup.select('.function_icon.clicked')[0].text
    except Exception as e:
        popular = ''
        print 'error4',link
    try:
        href = link
    except Exception as e:
        print 'error5',link
    #抓新聞按讚數
    link = link.replace('politics','new')+'/'
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
    doc = {'author':'蘋果日報','date':date,'title':title,'content':content,'popular':popular,'href':href,'share_count':share_count,'like_count':like_count,'comments':comments,'keywords':keywords}
    collect.insert_one(doc)
    '''
    #存進 pgdb
    function.keywords_insert_pgdb(keywords)
    function.kw_relation_insert_pgdb(keywords)
    function.doc_insert_pgdb(doc,1,2) #doc,source,big_source
    function.doc_join_kw_insert_pgdb(keywords,href)
    function.daily_kw_insert_pgdb(keywords,date,1) #keywords,date,source_fk
    '''
    print '%d/%d'%(ind1,t1)
    ind1 += 1

ind2 = 1
t2 = len(crawled_new_links)
for link in crawled_new_links:
    #更新已抓過的文章的人氣數
    try:
        res = requests.get(link)
    except Exception as e:
        print e
        continue
    soup = BeautifulSoup(res.text)
    try:
        popular = soup.select('.function_icon.clicked')[0].text
    except Exception as e:
        popular = ''
        print 'error6',link
    href = link
    #更新已抓過的文章的按讚數
    link = link.replace('politics','new')+'/'
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
        collect.update_one({'href':href},{'$set':{'share_count':share_count,'like_count':like_count,'popular':popular}})
    else:
        collect.update_one({'href':href},{'$set':{'share_count':share_count,'like_count':like_count,'comments':comments,'popular':popular}})
    #更新 doc in pgdb
    #function.doc_update_pgdb(document_link=href,document_like=like_count,document_share=share_count,fb_comments=comments)
    
    print '%d/%d'%(ind2,t2)
    ind2 += 1

print 'success'
print '新爬的文章數:',len(nocrawled_new_links)
print '更新的文章數:',len(crawled_new_links)
function.close_pgdb()

