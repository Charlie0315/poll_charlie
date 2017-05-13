
# coding: utf-8

# In[1]:

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import json
import urllib
import function

def get_ct_comments(link):
    encodeurl = urllib.quote_plus(link)
    res = requests.get('http://disqus.com/embed/comments/?base=default&version=fc270dd69992ff5c0b9e3819535930cd&f=chinatimes-news&t_u=%s'%encodeurl)
    soup = BeautifulSoup(res.text)
    data = json.loads(soup.select('#disqus-threadData')[0].text)
    ct_comments = []
    for post in data['response']['posts']:
        comment = {}
        comment['created_time'] = post['createdAt']
        comment['name'] = post['author']['name']
        comment['message'] = post['raw_message']
        ct_comments.append(comment)
    return ct_comments

client = MongoClient()
db = client['poll_charlie']
collect = db['chinatimes_news']

i = 0
crawled_new_links=[]
nocrawled_new_links=[]
    
#抓出所有文章連結
while True:
    i+=1
    res = requests.get('http://www.chinatimes.com/realtimenews/260407?page='+str(i))
    soup = BeautifulSoup(res.text)
    #該頁沒有新聞
    if soup.select('h2 a') == []:
        break
    for entry in soup.select('h2 a'):
        link = 'http://www.chinatimes.com'+ entry['href']
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
        title = soup.select('article header h1')[0].text
    except Exception as e:
        title = ''
        print 'error2',link
    try:
        content = ''
        for text in soup.select('figcaption'):
            content += text.text  
        for text in soup.select('article article p'):
            content += text.text    
    except Exception as e:
        content = ''
        print 'error3',link
    try:
        popular = soup.select('.art_click .num')[0].text
    except Exception as e:
        popular = ''
        print 'error4',link
    try:
        href = link
    except Exception as e:
        href = ''
        print 'error6',link
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
    #抓中時 comments
    try:
        ct_comments = get_ct_comments(link)
    except:
        ct_comments = []
        print '抓不到ct_comments'
    #提取文本關鍵字
    keywords = function.keyword_extract(content)
    #存進 mongodb
    doc = {'author':'中國時報','date':date,'share_count':share_count,'like_count':like_count,'comments':comments,'title':title,'content':content,'popular':popular,'href':href,'ct_comments':ct_comments,'keywords':keywords}
    collect.insert_one(doc)
    #存進 pgdb
    #'''
    function.keywords_insert_pgdb(keywords)
    function.kw_relation_insert_pgdb(keywords)
    function.doc_insert_pgdb(doc,4,2) #doc,source,big_source
    function.doc_join_kw_insert_pgdb(keywords,href)
    function.daily_kw_insert_pgdb(keywords,date,4) #keywords,date,source_fk
    #    time.sleep(1)
    #'''
    print '%d/%d'%(ind1,t1)
    ind1 += 1
    
#更新已抓過的文章的人氣數
ind2 = 1
t2 = len(crawled_new_links)
for link in crawled_new_links:
    res = requests.get(link)
    soup = BeautifulSoup(res.text)
    try:
        popular = soup.select('.art_click .num')[0].text
    except Exception as e:
        popular = ''
        print 'error7',link
    #抓中時 comments
    try:
        ct_comments = get_ct_comments(link)
    except Exception as e:
        ct_comments = []
        print e
        print '抓不到ct_comments'
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
    #更新 mongodb
    collect.update_one({'href':link},{'$set':{'ct_comments':ct_comments,'popular':popular,'share_count':share_count,'like_count':like_count,'comments':comments}})
    #更新 doc in pgdb
    function.doc_update_pgdb(document_link=link,ct_comments=ct_comments,document_like=like_count,document_share=share_count,fb_comments=comments)        
    
    print '%d/%d'%(ind2,t2)
    ind2 += 1
print 'success'
print '新爬的文章數:',len(nocrawled_new_links)
print '更新的文章數:',len(crawled_new_links)
function.close_pgdb()

