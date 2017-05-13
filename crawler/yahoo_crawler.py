
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
collect = db['yahoo_news']

nocrawled_new_links=[]
crawled_new_links=[]

links = ['https://tw.news.yahoo.com/politics/archive/%d.html'%i for i in range(1,41)]
#抓出所有文章連結
for entry1 in links:
    try:
        res = requests.get(entry1)
    except Exception as e:
        print e
        continue
    soup = BeautifulSoup(res.text)
    for entry2 in soup.select('.yom-list-wide.thumbnail li'):
        link = 'https://tw.news.yahoo.com'+entry2.select('a')[0]['href']
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
        date = soup.select('.byline.vcard abbr')[0]['title']
    except Exception as e:
        date = None
        print 'error1',link
    try:
        title = soup.select('.headline')[0].text
    except Exception as e:
        title = ''
        print 'error2',link
    try:
        content = soup.select('.yom-mod.yom-art-content .bd')[0].text.strip()
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
    keywords = function.keyword_extract(content)
    #存進 mongodb
    doc = {'author':'Yahoo!奇摩新聞','date':date,'title':title,'content':content,'href':href,'share_count':share_count,'like_count':like_count,'comment_count':comment_count,'keywords':keywords}
    collect.insert_one(doc)
    #存進 pgdb
    function.keywords_insert_pgdb(keywords)
    function.kw_relation_insert_pgdb(keywords)
    function.doc_insert_pgdb(doc,47,2) #doc,source,big_source
    function.doc_join_kw_insert_pgdb(keywords,href)
    function.daily_kw_insert_pgdb(keywords,date,47) #keywords,date,source_fk
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

