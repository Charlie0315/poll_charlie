
# coding: utf-8

# In[1]:

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import json
import random
import function
import jieba.analyse

client = MongoClient()
db = client['poll_charlie']
collect = db['ptt_gossiping']

crawled_art_links=[]
nocrawled_art_links=[]

payload = {
'from':'/bbs/Gossiping/index.html',
'yes':'yes'
}
rs = requests.session()
res = rs.post('https://www.ptt.cc/ask/over18',data=payload)
#抓出所有文章連結
link = 'https://www.ptt.cc/bbs/Gossiping/index.html'
res = rs.get(link)
soup = BeautifulSoup(res.text)
newest = int(soup.select('.btn.wide')[1]['href'].split('.html')[0].split('index')[1]) #政黑最新一頁index

for i in range(150):
    link = 'https://www.ptt.cc/bbs/Gossiping/index%s.html'%newest
    newest -= 1
    try:
        res = rs.get(link)
    except Exception as e:
        print e
        continue
    soup = BeautifulSoup(res.text)
    
    for entry in soup.select('.r-ent a'):
        link = 'https://www.ptt.cc'+ entry['href']
        #分成已爬過的連結跟未爬過的連結 (以postgresql為主)
        if function.iscrawled(link):
            crawled_art_links.append(link)
        else:
            nocrawled_art_links.append(link)
        '''
        #分成已爬過的連結跟未爬過的連結 (以mongodb為主)
        if collect.find_one({'href':link}):
            crawled_art_links.append(link)
        else:
            nocrawled_art_links.append(link)
        '''
            
#抓未爬過的文章內容
ind1 = 1
t1 = len(nocrawled_art_links)
for link in nocrawled_art_links:
    j = 0
    while True:
        try:
            res = rs.get(link)
            break
        except Exception as e:
            j += 1
            if j == 10:
                break
            continue
    soup = BeautifulSoup(res.text)
    try:
        author = soup.select('.article-metaline .article-meta-value')[0].text
        title = soup.select('.article-metaline .article-meta-value')[1].text
        date = soup.select('.article-metaline .article-meta-value')[2].text
        date = time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(date,'%a %b %d %H:%M:%S %Y')) #Sun Nov 22 22:21:23 2015轉換成 2015-11-22 22:21:23
        date = date+'+08'
    except Exception as e:
        author = ''
        title = ''
        date = None
        print 'error1',link
    try:
        push=[]
        like_count = 0
        hiss = 0
        for entry in soup.select('.push'):
            push_tag = entry.select('.push-tag')[0].text.strip()
            if push_tag == u'推':
                like_count += 1
            if push_tag == u'噓':
                hiss += 1
            push_userid = entry.select('.push-userid')[0].text
            push_content = entry.select('.push-content')[0].text
            push_datetime = entry.select('.push-ipdatetime')[0].text
            push.append({'push_tag':push_tag,'push_userid':push_userid,'push_content':push_content,'push_datetime':push_datetime})
    except Exception as e:
        push = []
        like_count = 0
        hiss = 0
        print 'error2',link
    try:
        a = soup.select('#main-content')[0].text.split(u'--\n※ 發信站: 批踢踢實業坊(ptt.cc)')
        b = a[0].split('\n',1)
        content = b[1]
    except Exception as e:
        content = ''
        print 'error3',link
    try:
        href = link
    except Exception as e:
        print 'error4',link
    #提取文本關鍵字
    keywords = jieba.analyse.textrank(content,5,allowPOS=('n','ng','nr','nrfg','nrt','ns','nt','nz'))
    doc = {'like_count':like_count,'hiss':hiss,'author':author,'title':title,'date':date,'content':content,'push':push,'href':href,'keywords':keywords}  
    #存進 mongodb
    collect.insert_one(doc)
    #'''
    #存進 pgdb
    function.keywords_insert_pgdb(keywords)
    function.kw_relation_insert_pgdb(keywords)
    function.doc_insert_pgdb(doc,51,1) #doc,source,big_source
    function.doc_join_kw_insert_pgdb(keywords,href)
    function.daily_kw_insert_pgdb(keywords,date,51) #keywords,date,source_fk
    #'''
    print '%d/%d'%(ind1,t1)
    ind1 += 1
    
#更新已抓過的推文
ind2 = 1
t2 = len(crawled_art_links)
for link in crawled_art_links:
    #若重複連線10次失敗則跳過此篇文章
    j = 0
    while True:
        try:
            res = rs.get(link)
            break
        except Exception as e:
            j += 1
            if j == 10:
                break
            continue
    soup = BeautifulSoup(res.text)
    push=[]
    try:
        for entry in soup.select('.push'):
            push_tag = entry.select('.push-tag')[0].text
            push_userid = entry.select('.push-userid')[0].text
            push_content = entry.select('.push-content')[0].text
            push_datetime = entry.select('.push-ipdatetime')[0].text
            push.append({'push_tag':push_tag,'push_userid':push_userid,'push_content':push_content,'push_datetime':push_datetime})
    except Exception as e:
        print 'error5',link
    collect.update_one({'href':link},{'$set':{'push':push}})
    #更新 doc in pgdb
    #function.doc_update_pgdb(document_link=link,push=push)
    
    print '%d/%d'%(ind2,t2)
    ind2 += 1
print '新爬的文章數:',len(nocrawled_art_links)
print '更新的文章數:',len(crawled_art_links)
print 'success'
function.close_pgdb()

