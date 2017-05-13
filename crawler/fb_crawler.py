
# coding: utf-8

# In[ ]:

import requests
import json
import datetime
import time
import function
import psycopg2
from pymongo import MongoClient

token = 'CAALtvXfI7N4BAEj5iSZATMFLq1hsKgjdlBo5zg2xlAqFII0PRWBoZCz1LuKMUCk75DAxfUZCFkZAcZAlNL0GQjf4dl9LZAEhmod9K2NOLHTTZCT6XdnLH6LNdEtkZAZBaO139CIBnpYcNK3imgUv7iVTGj2cMfeRKF3AlT5Mgx2fxZAwo8ahtYZBjCGt9n8Im1DnSVnNaHDvrxshwHxT7T7PNCz'
conn = psycopg2.connect("dbname='polldb_bak' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()

def get_like_count(post_id): #取得按讚數
    likes_url = 'https://graph.facebook.com/v2.3/%s/likes?summary=true&access_token=%s'%(post_id,token)
    res = requests.get(likes_url)
    likes = json.loads(res.text)
    try:
        like_count = likes['summary']['total_count']
    except Exception as e:
        like_count = 0
        print '沒有按讚數',post_id
    return like_count
def get_like_list(post_id): #取得按讚清單
    likes_url = 'https://graph.facebook.com/v2.3/%s/likes?limit=1000&summary=true&access_token=%s'%(post_id,token)
    like_list = []
    while True:
        res = requests.get(likes_url)
        likes = json.loads(res.text)
        try:
            like_list += likes['data']
        except Exception as e:
            print '沒有按讚清單',post_id
        try:
            likes_url = likes['paging']['next']
        except Exception as e:
            break
    return like_list
def get_shared_list(post_id):
    shared_data = []
    sharedpost_id = post_id.split('_')[1]
    shared_url = 'https://graph.facebook.com/v2.3/%s/sharedposts?limit=100&access_token=%s'%(sharedpost_id,token)
    while True:
        res = requests.get(shared_url)
        for sharedpost in json.loads(res.text)['data']:
            shared_one_data = {}
            shared_one_data['post_id'] = sharedpost['id']
            shared_one_data['user_id'] = sharedpost['from']['id']
            shared_one_data['user_name'] = sharedpost['from']['name']
            shared_one_data['created_time'] = sharedpost['created_time']
            shared_data.append(shared_one_data)
        try:
            shared_url = sharedpost['paging']['next']
        except Exception as e:
            break
    return shared_data
def get_comment_list(post_id):
    comments_data = []
    comments_url = 'https://graph.facebook.com/v2.3/%s/comments?limit=100&summary=true&access_token=%s'%(post_id,token)
    while True:
        time.sleep(1)
        res = requests.get(comments_url)
        comments = json.loads(res.text)
        try:
            c = comments['data']
        except Exception as e:
            print '抓fb_comments出錯',e
            return comments_data
        for comment in c:
            comment_one_data = {}
            comment_one_data['likes'] = []
            comment_one_data['id'] = comment['id']
            comment_one_data['user_id'] = comment['from']['id']
            comment_one_data['user_name'] = comment['from']['name']
            comment_one_data['message'] = comment['message']
            comment_one_data['likes_count'] = comment['like_count']
            comment_one_data['created_time'] = comment['created_time']
            #comment_one_data['likes'] = get_like_list(comment['id'])
            #取得留言的回覆清單
            comment_one_data['comment_reponse'] = get_comment_list(comment['id'])
            comments_data.append(comment_one_data)
        try:
            comments_url = comments['paging']['next']
        except Exception as e:
            break
    return comments_data
        
client = MongoClient()
db = client['poll_charlie']
collect = db['fb_fanpages']

page_data = {}
posts_data = []
cur.execute("SELECT page_id FROM article_page_join_keyword")
page_ids = cur.fetchall()
for page_id in page_ids:
    #取得粉絲團資訊
    res = requests.get('https://graph.facebook.com/v2.3/%s?access_token=%s'%(page_id[0],token))
    page = json.loads(res.text)
    try:
        print u"粉專id:"+page['id']
        print u"粉專名稱:"+page['name']
    print '--------------------------------------------------------------------------------'
    posts_url = 'https://graph.facebook.com/v2.3/%s/posts?limit=7&access_token=%s'%(page_id[0],token)
    time.sleep(1)
    res = requests.get(posts_url)
    try:
        posts = json.loads(res.text)['data']
    except:
        print '載入posts出錯',res.text
        continue
    #取得貼文id list
    nocrawled_post_ids = []
    crawled_post_ids = []
    for post in posts:
        link = 'https://www.facebook.com/'+post['id']
        if function.iscrawled(link):
            crawled_post_ids.append(post['id'])
        else:
            nocrawled_post_ids.append(post['id'])
    #取得每篇未爬過貼文詳細資料
    for post_id in nocrawled_post_ids:
        post_one_data = {}
        post_one_data['comments'] = []
        post_one_data['likes'] = []
        post_one_data['shared'] =[]
        time.sleep(1)
        res = requests.get('https://graph.facebook.com/v2.3/%s?access_token=%s'%(post_id,token))
        post = json.loads(res.text)
        post_one_data['id'] = post['id'] #id
        post_one_data['href'] = 'https://www.facebook.com/'+post_id #href
        post_one_data['author'] = page['name'] #author
        try:
            post_one_data['message'] = post['message'] #內容
        except Exception as e:
            post_one_data['message'] = ''
            print '缺少message',post_id
        post_one_data['title'] = post_one_data['message'].split('\n')[0] #title
        date = post['created_time'].replace('T',' ')
        date = date.split('+')[0]
        post_one_data['date'] = date #創建時間 (UTC+00)
        print post_one_data['date']
        try:
            post_one_data['share_count'] = post['shares']['count'] #分享數
        except Exception as e:
            post_one_data['share_count'] = 0
            print '沒有分享數',post_id

        post_one_data['like_count'] = get_like_count(post_id)
        #post_one_data['likes'] = get_like_list(post_id)
        #post_one_data['shared'] = get_shared_list(post_id) #分享名單
        post_one_data['comments'] = get_comment_list(post_id)
        #提取文本關鍵字
        keywords = function.keyword_extract(post_one_data['message'])
        post_one_data['keywords'] = keywords
        #存進 mongodb
        collect.insert_one(post_one_data)
        #存進 pgdb
        function.keywords_insert_pgdb(keywords)
        function.kw_relation_insert_pgdb(keywords)
        function.doc_insert_pgdb(post_one_data,18,3) #doc,source,big_source
        function.doc_join_kw_insert_pgdb(keywords,post_one_data['href'])
        function.daily_kw_insert_pgdb(keywords,post_one_data['date'],18) #keywords,date,source_fk
        function.fb_doc_relation_keyword(post_one_data['href'],page['id']) #某粉絲團貼文與該粉絲團關聯一起

    #更新貼文
    for post_id in crawled_post_ids:
        res = requests.get('https://graph.facebook.com/v2.3/%s?access_token=%s'%(post_id,token))
        post = json.loads(res.text)
        href = 'https://www.facebook.com/'+post_id #href
        try:
            share_count = post['shares']['count'] #分享數
        except Exception as e:
            share_count = 0
            print '沒有分享數',post_id
        like_count = get_like_count(post_id)
        comments = get_comment_list(post_id)
        #更新 mongodb
        collect.update_one({'href':href},{'$set':{'share_count':share_count,'like_count':like_count,'comments':comments}})
        #更新 pgdb
        function.doc_update_pgdb(document_link=href,document_like=like_count,document_share=share_count,fb_comments=comments)
    print '新爬的文章數:',len(nocrawled_post_ids)
    print '更新的文章數:',len(crawled_post_ids)
cur.close()
conn.close()
print 'success'

