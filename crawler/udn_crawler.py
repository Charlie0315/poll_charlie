
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
collect = db['udn_news']

crawled_new_links=[]
nocrawled_new_links=[]

#抓出所有文章連結
res = requests.get('http://udn.com/news/cate/6638')
res.encoding = 'utf-8'
soup = BeautifulSoup(res.text)

for entry1 in soup.select('.category_box'):
    if entry1.select('h3')[0].text.strip() == u'政治' or entry1.select('h3')[0].text.strip() == u'2016大選觀測站' or entry1.select('h3')[0].text.strip() == u'立委選情漸升溫':
        for entry in entry1.select('.category_box_list a'):
            link = 'http://udn.com'+ entry['href']
            link = link.split('-')[0]
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
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text)
    try:
        date = soup.select('#story_bady_info h3')[0].text
        date = re.findall('\d{4}-\d{2}-\d{2} \d{2}:\d{2}',date)[0]+'+08'
    except Exception as e:
        date = None
        print 'error1',link
    try:
        title = soup.select('#story_art_title')[0].text
    except Exception as e:
        title = ''
        print 'error2',link
    try:
        content = soup.select('#story_body_content')[0].text.split('   ')[-1].strip()
    except Exception as e:
        content = ''
        print 'error3',link
    try:
        href = link
    except Exception as e:
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
    #抓新聞的 udn comments
    article_id = link.split('/')[-1]
    fp = 1
    udn_comments = []
    while True:
        res = requests.get('http://func.udn.com/funcap/discuss/disList.jsp?article_id=%s&channel_id=2&fp=%d'%(article_id,fp))
        try:
            a = re.findall('var dislist= \[(.*)\] ;',res.text)[0]
        except:
            print 'http://func.udn.com/funcap/discuss/disList.jsp?article_id=%s&channel_id=2&fp=%d'%(article_id,fp)
            print res.text
            break
        if a == '':
            break
        fp += 1
        for i in range(len(re.findall('\"userId\" : \"(.*?)\"',a))):
            comment = {}
            comment['id'] = re.findall('\"userId\" : \"(.*?)\"',a)[i]
            comment['message'] = re.findall('\"content\" : \"(.*?)\"',a)[i]
            comment['date'] = re.findall('\"postDate\" : \"(.*?)\"',a)[i]
            udn_comments.append(comment)
    #提取文本關鍵字
    keywords = function.keyword_extract(content)
    #存進 mongodb
    doc = {'author':'聯合報','date':date,'title':title,'content':content,'href':href,'share_count':share_count,'like_count':like_count,'comments':comments,'udn_comments':udn_comments,'keywords':keywords}
    collect.insert_one(doc)
    #存進 pgdb
    '''
    try:
        function.keywords_insert_pgdb(keywords)
        function.kw_relation_insert_pgdb(keywords)
        function.doc_insert_pgdb(doc,2,2) #doc,source,big_source
        function.doc_join_kw_insert_pgdb(keywords,href)
        function.daily_kw_insert_pgdb(keywords,date,2) #keywords,date,source_fk
    except Exception as e:
        print e
        pass
    #time.sleep(1)
    '''
    print '%d/%d'%(ind1,t1)
    ind1 += 1
    
ind2 = 1
t2 = len(crawled_new_links)
for link in crawled_new_links:
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
    #更新已抓過的文章的 udn comments
    article_id = link.split('/')[-1]
    fp = 1
    udn_comments = []
    while True:
        res = requests.get('http://func.udn.com/funcap/discuss/disList.jsp?article_id=%s&channel_id=2&fp=%d'%(article_id,fp))
        try:
            a = re.findall('var dislist= \[(.*)\] ;',res.text)[0]
        except:
            print 'http://func.udn.com/funcap/discuss/disList.jsp?article_id=%s&channel_id=2&fp=%d'%(article_id,fp)
            print res.text
            break
        if a == '':
            break
        fp += 1
        for i in range(len(re.findall('\"userId\" : \"(.*?)\"',a))):
            comment = {}
            comment['id'] = re.findall('\"userId\" : \"(.*?)\"',a)[i]
            comment['message'] = re.findall('\"content\" : \"(.*?)\"',a)[i]
            comment['date'] = re.findall('\"postDate\" : \"(.*?)\"',a)[i]
            udn_comments.append(comment)
    #更新 mongodb
    collect.update_one({'href':link},{'$set':{'share_count':share_count,'like_count':like_count,'comments':comments,'udn_comments':udn_comments}})
    #更新 doc in pgdb
    #function.doc_update_pgdb(document_link=link,document_like=like_count,document_share=share_count,fb_comments=comments,udn_comments=udn_comments)

    print '%d/%d'%(ind2,t2)
    ind2 += 1
print 'success'
print '新爬的文章數:',len(nocrawled_new_links)
print '更新的文章數:',len(crawled_new_links)
function.close_pgdb()

