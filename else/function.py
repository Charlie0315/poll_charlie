
# coding: utf-8

# In[2]:

import requests
from bs4 import BeautifulSoup
import time
import json
import urllib
import re
import jieba.analyse
import jieba
import string
#from sklearn import feature_extraction
#from sklearn.feature_extraction.text import TfidfTransformer
#from sklearn.feature_extraction.text import CountVectorizer
import os
import psycopg2
import itertools
import pytz
from datetime import datetime
from datetime import timedelta

fetched_time = datetime.now(tz=pytz.utc)
Wfbc = 2
Wfbs = 4
access_token = 'EAACEdEose0cBAHHTxbIA14T0XfYEIURdLEA6uQGzyvTraNP7qfEMpLDJ0haYwoERU9lNw4JUM2XJ75ZA35D33K06XVPxBbbFKVM9xjqOuHt1V2k1FlcwIdVcZARYKtnZAvMOSRuhAwquQDnahliyMJrjW66bw9zhcciYypvu7jJuPsY8lnd1G2UyPVmMtYZD'


conn = psycopg2.connect("dbname='polldb_bak' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()

def get_fb_likes(url):
    encodeurl = urllib.quote_plus(url)
    res = requests.get('https://www.facebook.com/plugins/like.php?href=%s'%encodeurl)
    res.encoding = 'utf8'
    soup = BeautifulSoup(res.text)
    try:
        like_count = int(soup.select('#u_0_2')[0].text.split(' ')[0].replace(',',''))
    except Exception as e:
        like_count = 0
        print '沒有新聞按讚數',url
    return like_count
'''
def get_fb_comments(url):
    encodeurl = urllib.quote_plus(url)
    try:
        res = requests.get('https://www.facebook.com/plugins/comments.php?order_by:social&numposts=100&href=%s'%encodeurl)
    except Exception as e:
        res = requests.get('https://www.facebook.com/plugins/comments.php?order_by:social&numposts=100&href=%s'%encodeurl)
    res.encoding = 'utf8'
    comment_ids = re.findall('\"comments\"\:\{\"commentIDs\":\[(.*)\],"idMap"',res.text)[0].replace('"','').split(',')
    comments = []
    if comment_ids != [u'']:
        for comment_id in comment_ids:
            try:
                res = requests.get('https://graph.facebook.com/v2.3/%s?access_token=%s'%(comment_id,access_token))
                comment = json.loads(res.text)
                del comment['can_remove']
                del comment['user_likes']
            except Exception as e:
                print e
                print 'fb comments出錯',comment_id
                continue
            #抓 comments 的回覆
            try:
                res = requests.get('https://graph.facebook.com/v2.3/%s/comments?access_token=%s'%(comment_id,access_token))
                comment_response_data = json.loads(res.text)
                comment['comment_response'] = []
                for response in comment_response_data['data']:
                    del response['can_remove']
                    del response['user_likes']
                    comment['comment_response'].append(response)
            except Exception as e:
                comment['comment_response'] = []
                print 'fb reponse出錯','https://graph.facebook.com/v2.3/%s/comments?access_token=%s'%(comment_id,access_token)
            comments.append(comment)
    return comments
'''
def get_fb_comments(post_id):
    comments_data = []
    comments_url = 'https://graph.facebook.com/v2.3/%s/comments?limit=100&summary=true&access_token=%s'%(post_id,access_token)
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
            comment_one_data['user_id'] = comment['from']['id']
            comment_one_data['user_name'] = comment['from']['name']
            comment_one_data['message'] = comment['message']
            comment_one_data['likes_count'] = comment['like_count']
            comment_one_data['created_time'] = comment['created_time']
            #comment_one_data['likes'] = get_like_list(comment['id'])
            #取得留言的回覆清單
            comment_one_data['comment_reponse'] = get_fb_comments(comment['id'])
            comments_data.append(comment_one_data)
        try:
            comments_url = comments['paging']['next']
        except Exception as e:
            break
    return comments_data
    

#json檔轉成語料文字檔
def getfiles(s_path):
    with open(s_path,'rb') as f:
        source_data = json.load(f)
    a = s_path.split('/')[-1].split('_')[0]
    for new in source_data:
        b = new['href'].split('/')[-1]
        fname = a+'_'+b
        sFilePath = 'd:/charlie poll/%s_files/'%a
        if not os.path.exists(sFilePath): 
            os.mkdir(sFilePath)
        with open(sFilePath+'%s.txt'%fname,'wb') as f:
            f.write(new['content'].encode('utf8'))
            
def getFilelist(path) :
    files = os.listdir(path)
    return files,path
#把語料文字檔做斷詞
def fenci(argv,path) :
    #保存分词结果的目录
    sFilePath = 'd:/charlie poll/all_seg'
    if not os.path.exists(sFilePath) : 
        os.mkdir(sFilePath)
    #读取文档
    filename = argv
    with open(path+filename,'r+') as f:
        file_content = f.read()
    
    #对文档进行分词处理，采用默认模式
    seg_content = jieba.cut(file_content)

    #对空格，换行符进行处理
    result = []
    for seg in seg_content :
        seg = ''.join(seg.split())
        if (seg != '' and seg != "\n" and seg != "\n\n") :
            result.append(seg)
    #将分词后的结果用空格隔开，保存至本地。比如"我来到北京清华大学"，分词结果写入为："我 来到 北京 清华大学"
    with open(sFilePath+"/"+filename,"w+") as f:
        f.write(' '.join(result).encode('utf8'))

def Tfidf() :
    path = 'd:/charlie poll/all_seg/'
    corpus = []  #存取文档的分词结果
    filelist = os.listdir(path)
    for ff in filelist :
        fname = path + ff
        with open(fname,'r+') as f:
            content = f.read()
        corpus.append(content)    

    vectorizer = CountVectorizer()    
    transformer = TfidfTransformer()
    tfidf = transformer.fit_transform(vectorizer.fit_transform(corpus))
    
    word = vectorizer.get_feature_names() #所有文本的关键字
    weight = tfidf.toarray()              #对应的tfidf矩阵
    sum_weight = []
    for i in range(len(word)):
        one_word = 0
        for j in range(len(weight)):
            one_word += weight[j][i]
        sum_weight.append(one_word)
    
    sFilePath = 'd:/charlie poll/tfidffile'
    if not os.path.exists(sFilePath): 
        os.mkdir(sFilePath)
     
    '''
    # 这里将每份文档词语的TF-IDF写入tfidffile文件夹中保存
    for i in range(len(weight)) :
        print u"--------Writing all the tf-idf in the",i,u" file into ",sFilePath+'/'+string.zfill(i,5)+'.txt',"--------"
        f = open(sFilePath+'/'+string.zfill(i,5)+'.txt','w+')
        for j in range(len(word)) :
            f.write(word[j].encode('utf8')+" "+str(weight[i][j])+"\n")
        f.close()
    '''
    with open(sFilePath+'/idf.txt','w+') as f:
        for j in range(len(word)) :
            f.write(word[j].encode('utf8')+" "+str(sum_weight[j])+"\n")

def keyword_extract(text):
    return jieba.analyse.textrank(text,10,allowPOS=('n','ng','nr','nrfg','nrt','ns','nt','nz'))

def keywords_insert_pgdb(keywords):
    for kw in keywords:
        cur.execute("SELECT * FROM article_keyword WHERE kw_name=(%s)",(kw,))
        if cur.fetchall() == []:
            cur.execute("INSERT INTO article_keyword(kw_name) VALUES (%s)",(kw,))
    conn.commit()
    
def doc_insert_pgdb(doc,source_fk,big_source_fk):
    document_link = doc['href']
    try:
        document_author = doc['author']
    except:
        document_author = ''
    try:
        document_title = doc['title']
    except:
        document_title = ''
    try:
        document_date = doc['date']
    except:
        document_date = None
    try:
        document_like = doc['like_count']
    except:
        document_like = 0
    try:
        document_share = doc['share_count']
    except:
        document_share = 0
    try:
        document_hiss = doc['hiss']
    except:
        document_hiss = None
    try:
        fb_comments = len(doc['comments'])
    except:
        fb_comments = 0
    try:
        udn_comments = len(doc['udn_comments'])
    except:
        udn_comments = 0
    try:
        ct_comments = len(doc['ct_comments'])
    except:
        ct_comments = 0    
    try:
        push = len(doc['push'])
    except:
        push = 0
    try:
        document_message_number = doc['comment_count']
    except:
        document_message_number = fb_comments+udn_comments+ct_comments+push
    document_value = document_like+document_message_number*Wfbc+document_share*Wfbs
    cur.execute("""INSERT INTO article_document(document_hiss,document_link,source_fk_id,big_source_fk_id,document_author,document_title,document_date,document_like,document_share,document_message_number,document_value,fetched_time) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (document_hiss,document_link,source_fk,big_source_fk,document_author,document_title,document_date,document_like,document_share,document_message_number,document_value,fetched_time))
    conn.commit()
    #計算爆發力
    if document_date != None:
        cur.execute("SELECT document_date FROM article_document WHERE document_link=%s",(document_link,))
        old_fetched_time = cur.fetchone()[0]
        hours = (fetched_time - old_fetched_time).seconds/3600
        if hours != 0:
            burst = document_value / hours
            cur.execute("""UPDATE article_document SET burst=%s WHERE document_link=%s""",(burst,document_link))
            conn.commit()
    
def doc_update_pgdb(document_link,comment_count=0,document_like=0,document_share=0,fb_comments=[],udn_comments=[],ct_comments=[],push=[]):
    if comment_count != 0:
        document_message_number = comment_count
    else:
        document_message_number = len(fb_comments)+len(udn_comments)+len(ct_comments)+len(push)
    document_value = document_like+document_message_number*Wfbc+document_share*Wfbs
    #計算爆發力
    cur.execute("SELECT document_value,fetched_time FROM article_document WHERE document_link=%s",(document_link,))
    result = cur.fetchone()
    old_document_value = result[0]
    old_fetched_time = result[1]
    hours = (fetched_time - old_fetched_time).seconds/3600
    if hours != 0:
        burst = (document_value - old_document_value)/hours
        cur.execute("""UPDATE article_document
                        SET document_like=%s,document_share=%s,document_message_number=%s,document_value=%s,burst=%s,fetched_time=%s
                        WHERE document_link=%s""",(document_like,document_share,document_message_number,document_value,burst,fetched_time,document_link))
        conn.commit()

def doc_join_kw_insert_pgdb(keywords,url):
    kw_pks = kw_name_convert_to_kw_pk(keywords)
    cur.execute("SELECT document_pk FROM article_document WHERE document_link=(%s)",(url,))
    doc_pk = cur.fetchall()[0][0]
    for kw_pk in kw_pks:
        cur.execute("""INSERT INTO article_document_join_keyword(document_fk_id,keyword_fk_id) 
                        VALUES(%s,%s)""",(doc_pk,kw_pk))
    conn.commit()
        
def kw_name_convert_to_kw_pk(keywords):
    kw_pks = []
    for kw in keywords:
        cur.execute("SELECT kw_pk FROM article_keyword WHERE kw_name=(%s)",(kw,))
        kw_pks.append(cur.fetchall()[0][0])
    return kw_pks
    
def kw_relation_insert_pgdb(keywords):
    #kw_name轉換成 kw_pk
    kw_pks = kw_name_convert_to_kw_pk(keywords)
    for i in itertools.combinations(kw_pks,2):
        cur.execute("""SELECT keyword_relation_pk,keyword_relation_value FROM article_keyword_relation 
                        WHERE (keyword_fk_one=%s AND keyword_fk_two=%s) 
                        OR (keyword_fk_one=%s AND keyword_fk_two=%s)""",(i[0],i[1],i[1],i[0]))
        result = cur.fetchall()
        if result == []:
            cur.execute("""INSERT INTO article_keyword_relation(keyword_fk_one,keyword_fk_two)
                        VALUES (%s,%s)""",i)
        else:
            cur.execute("""UPDATE article_keyword_relation
                            SET keyword_relation_value=%s
                            WHERE keyword_relation_pk=%s""",(result[0][1]+1,result[0][0]))
        conn.commit()

def daily_kw_insert_pgdb(keywords,date,source):
    try:
        if u' ' in date:
            date = date.split(' ')[0] + '+08'
        else:
            pass
    except:
        return None
    for kw in keywords:
        cur.execute("SELECT daily_kw_pk,kw_name,date,count FROM article_daily_keyword WHERE kw_name=%s AND date=%s AND source_fk=%s",(kw,date,source))
        result = cur.fetchall()
        if result == []:
            #看這個 keyword是否需要被過濾掉
            cur.execute("SELECT * FROM article_daily_keyword_hide WHERE kw_name=%s",(kw,))
            if cur.fetchall() == []:
                hide = 0 #不過濾
            else:
                hide = 1 #過濾
            cur.execute("INSERT INTO article_daily_keyword(kw_name,date,source_fk,hide) VALUES(%s,%s,%s,%s)",(kw,date,source,hide))
        else:
            cur.execute("UPDATE article_daily_keyword SET count=%s WHERE daily_kw_pk=%s",(result[0][3]+1,result[0][0]))
        conn.commit()

def iscrawled(link):
    cur.execute("SELECT * FROM article_document WHERE document_link=%s",(link,))
    if cur.fetchall() != []:
        return True
    else:
        return False

def fb_doc_relation_keyword(link,page_id):
    cur.execute("SELECT document_pk FROM article_document WHERE document_link = %s",(link,))
    doc_pk = cur.fetchone()[0]
    cur.execute("SELECT kw_fk_id FROM article_page_join_keyword WHERE page_id = %s",(page_id,))
    kw_pk = cur.fetchone()[0]
    cur.execute("SELECT * FROM article_document_join_keyword WHERE document_fk_id=%s AND keyword_fk_id=%s",(doc_pk,kw_pk))
    if cur.fetchall() == []:
        cur.execute("INSERT INTO article_document_join_keyword(document_fk_id,keyword_fk_id) VALUES(%s,%s)",(doc_pk,kw_pk))
    conn.commit()

def close_pgdb():
    cur.close()
    conn.close()