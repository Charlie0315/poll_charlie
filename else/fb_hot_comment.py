
# coding: utf-8

# In[1]:

#抓出粉專當天PO文三則最熱門回應
import requests
import json
import re
import psycopg2
import function
from operator import itemgetter
from datetime import datetime
from datetime import timedelta

token = 'CAALtvXfI7N4BAEj5iSZATMFLq1hsKgjdlBo5zg2xlAqFII0PRWBoZCz1LuKMUCk75DAxfUZCFkZAcZAlNL0GQjf4dl9LZAEhmod9K2NOLHTTZCT6XdnLH6LNdEtkZAZBaO139CIBnpYcNK3imgUv7iVTGj2cMfeRKF3AlT5Mgx2fxZAwo8ahtYZBjCGt9n8Im1DnSVnNaHDvrxshwHxT7T7PNCz'

def get_like_count(post_id): #取得按讚數
    likes_url = 'https://graph.facebook.com/v2.5/%s/likes?summary=true&access_token=%s'%(post_id,token)
    res = requests.get(likes_url)
    likes = json.loads(res.text)
    try:
        like_count = likes['summary']['total_count']
    except Exception as e:
        like_count = 0
        print '沒有按讚數',post_id
    return like_count
def get_like_list(post_id): #取得按讚清單
    likes_url = 'https://graph.facebook.com/v2.5/%s/likes?limit=1000&summary=true&access_token=%s'%(post_id,token)
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
    shared_url = 'https://graph.facebook.com/v2.5/%s/sharedposts?limit=100&access_token=%s'%(sharedpost_id,token)
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
            try:
                comment_one_data['user_id'] = comment['from']['id']
                comment_one_data['user_name'] = comment['from']['name']
                comment_one_data['message'] = comment['message']
                comment_one_data['like_count'] = comment['like_count']
                comment_one_data['created_time'] = comment['created_time']
                #comment_one_data['likes'] = get_like_list(comment['id'])
            except Exception as e:
                print '抓comment出錯:',comment['id']
                continue
            #取得留言的回覆清單
            comment_one_data['comment_reponse'] = get_comment_list(comment['id'])
            comment_one_data['recomment_count'] = len(comment_one_data['comment_reponse'])
            recomment_like_count = 0
            for recomment in comment_one_data['comment_reponse']:
                recomment_like_count += recomment['like_count']
            comment_one_data['recomment_like_count'] = recomment_like_count
            comments_data.append(comment_one_data)
        try:
            comments_url = comments['paging']['next']
        except Exception as e:
            break
    return comments_data
        
conn = psycopg2.connect("dbname='polldb_bak' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()

#page_data = {}
#posts_data = []
page_ids = ['109249609124014','232633627068','124616330906800','394896373929368']
cur.execute("SELECT hashtag_name FROM fb_hashtag")
hashtags = set([i[0].decode('utf8') for i in cur.fetchall()])
cur.execute("SELECT attag_name FROM fb_attag")
attags = set([i[0].decode('utf8') for i in cur.fetchall()])

start_date = (datetime.utcnow()-timedelta(days=1))

for page_id in page_ids:
    #取得粉絲團資訊
    res = requests.get('https://graph.facebook.com/v2.5/%s?access_token=%s'%(page_id,token))
    page = json.loads(res.text)
    print u"粉專id:"+page['id'] #page_id_fk
    print u"粉專名稱:"+page['name']
    print '--------------------------------------------------------------------------------'
    posts_url = 'https://graph.facebook.com/v2.3/%s/posts?limit=10&summary=true&access_token=%s'%(page_id,token)
    isover = False
    t = 0
    while True:
        res = requests.get(posts_url)
        posts = json.loads(res.text)
        try:
            a = posts['data']
        except:
            print '載入posts出錯',posts
            continue
        for post in a:
            try:
                message = post['message'] #message
            except:
                continue
            date = post['created_time'].replace('T',' ')
            date = date.split('+')[0]
            date = datetime.strptime(date,'%Y-%m-%d %H:%M:%S') #created_time
            if date < start_date:
                isover = True
                break
            try:
                post_tags = set([i['name'] for i in post['to']['data']])
            except:
                post_tags = set()
            post_hashgtags = set(re.findall('#(\S*) ',message))
            if ((hashtags & post_hashgtags) | (attags & post_tags)):
                post_id = post['id'] #post_id
                cur.execute("SELECT * FROM article_fb_post WHERE post_id_pk=%s",(post_id,))
                if cur.fetchone():
                    continue
                try:
                    description = post['description'] #description
                except:
                    description = ''
                try:
                    link = post['link'] #link
                except:
                    link = 'link:https://www.facebook.com/%s'%post['id']
                try:
                    share_count = post['shares']['count'] #share_count
                except:
                    share_count = 0
                    print '沒有share_count:',post_id
                like_count = get_like_count(post_id) #like_count
                comments = get_comment_list(post_id)
                comment_count = len(comments) #comment_count
                comments = sorted(comments, key=itemgetter('recomment_like_count'),reverse=True)
                post_recomment_like_count = 0
                for i in comments:
                    if i['recomment_like_count'] == 0:
                        break
                    post_recomment_like_count += i['recomment_like_count'] #post_recomment_like_count
                cur.execute("""INSERT INTO article_fb_post
                            (post_id_pk,post_link,message,like_count,share_count,comment_count,post_recomment_like_count,created_time,description,link,page_id_fk)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (post_id,'https://www.facebook.com/'+post_id,message,like_count,share_count,comment_count,post_recomment_like_count,date,description,link,page['id']))
                conn.commit() #insert post
                t += 1
                for i in comments[0:3]:
                    if i['message'] == '':
                        res = requests.get('https://graph.facebook.com/v2.5/%s?fields=attachment&access_token=%s'%(i['id'],token))
                        try:
                            i['message'] = json.loads(res.text)['attachment']['url']
                        except:
                            print 'no attachment:',i['id']
                    cur.execute("""INSERT INTO article_fb_comment
                                (comment_id_pk,message,like_count,recomment_count,recomment_like_count,post_id_fk)
                                VALUES(%s,%s,%s,%s,%s,%s)""",
                                (i['id'],i['message'],i['like_count'],i['recomment_count'],i['recomment_like_count'],post_id))
                    conn.commit() #insert comments
                #insert article_document
                href = 'https://www.facebook.com/'+post_id
                title = message.split('\n')[0]
                doc = {'href':href,'author':page['name'],'title':title,'date':date,'like_count':like_count,'share_count':share_count,'comment_count':comment_count}
                function.keywords_insert_pgdb(post_hashgtags)
                function.doc_insert_pgdb(doc,18,3)
                function.doc_join_kw_insert_pgdb(post_hashgtags,href)
        if isover:
            print 'insert %d posts'%t
            break
        try:
            posts_url = posts['paging']['next']
        except Exception as e:
            break

cur.close()
conn.close()
print 'success'

