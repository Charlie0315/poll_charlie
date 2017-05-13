
# coding: utf-8

# In[ ]:

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import json
import re
import function

client = MongoClient()
db = client['poll_charlie']
collect = db['LTN_news']

nocrawled_new_links=[]
crawled_new_links=[]

#抓最後一頁頁數
res = requests.get('http://news.ltn.com.tw/list/politics')
soup = BeautifulSoup(res.text)
last_page = int(soup.select('.p_last')[0]['href'].split('=')[-1])
#抓政治類每一頁連結
links = ['http://news.ltn.com.tw/list/politics?page='+str(i) for i in range(1,last_page+1)]

#抓出所有文章連結
for entry1 in links:
    try:
        res = requests.get(entry1)
    except Exception as e:
        print e
        continue
    soup = BeautifulSoup(res.text)
    for entry2 in soup.select('#newslistul')[0].select('a'):
        link = 'http://news.ltn.com.tw'+entry2['href']
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
        date = soup.select('#newstext span')[0].text
        date = date.replace(u'\xa0\xa0',' ')
        date = date+'+08'
    except Exception as e:
        date = None
        print 'error1',link
    try:
        title = soup.select('h1')[0].text
    except Exception as e:
        title = ''
        print 'error2',link
    try:
        content = ''
        for text in soup.select('#newstext p'):
            content += text.text
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
    #抓文章 comments
    try:
        comments = function.get_fb_comments(data['comments_fbid'])
    except Exception as e:
        comments = []
        print '抓取fb_comments錯誤',e
    #提取文本關鍵字
    keywords = function.keyword_extract(content)
    #存進 mongodb
    doc = {'author':'自由時報','date':date,'title':title,'content':content,'href':href,'share_count':share_count,'like_count':like_count,'comments':comments,'keywords':keywords}
    collect.insert_one(doc)
    '''
    #存進 pgdb
    function.keywords_insert_pgdb(keywords)
    function.kw_relation_insert_pgdb(keywords)
    function.doc_insert_pgdb(doc,3,2) #doc,source,big_source
    function.doc_join_kw_insert_pgdb(keywords,href)
    function.daily_kw_insert_pgdb(keywords,date,3) #keywords,date,source_fk
    '''
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
    #更新已抓過的文章的 comments
    try:
        comments = function.get_fb_comments(data['comments_fbid'])
    except Exception as e:
        print e
        comments = []
    #不讓空的 comments蓋掉舊的資料
    if comments == []:
        collect.update_one({'href':link},{'$set':{'share_count':share_count,'like_count':like_count}})
    else:
        collect.update_one({'href':link},{'$set':{'share_count':share_count,'like_count':like_count,'comments':comments}})
    #更新 doc in pgdb
    #function.doc_update_pgdb(document_link=link,document_like=like_count,document_share=share_count,fb_comments=comments)

    print '%d/%d'%(ind2,t2)
    ind2 += 1
    
print 'success'
print '新爬的文章數:',len(nocrawled_new_links)
print '更新的文章數:',len(crawled_new_links)
function.close_pgdb()


# In[4]:

import requests
from bs4 import BeautifulSoup

res = requests.get('https://www.luckydog.tw/search?s=iPhone&col=all&category=all&age=ongoing&sort=submitdate&phase=&page=7')
soup = BeautifulSoup(res.text)
for link in soup.select('.news-content'):
    print link.select('a')[0]['href']


# In[9]:

import requests
from bs4 import BeautifulSoup

res = requests.get('https://www.luckydog.tw/jsp/goto.jsp?pn=facebook_qek888_posts_event_20170325_28935')
soup = BeautifulSoup(res.text)
print soup.select('meta')[0]['content']
#print soup.select('#event-content-id a')[0]['href']

