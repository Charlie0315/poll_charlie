
# coding: utf-8

# In[ ]:

#標題向量餘弦相似度
import psycopg2
import itertools
import jieba
from sklearn.feature_extraction.text import CountVectorizer

def cosVector(x,y):
    result1=0.0
    result2=0.0
    result3=0.0
    for i in range(len(x)):
        result1+=x[i]*y[i]   #sum(X*Y)
        result2+=x[i]**2     #sum(X*X)
        result3+=y[i]**2     #sum(Y*Y)
    #print(result1)
    #print(result2)
    #print(result3)
    result = result1/(result2*result3)**0.5#结果显示
    return result

conn = psycopg2.connect("dbname='polldb' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()

vectorizer = CountVectorizer()
titles = []
links = []
cur.execute("SELECT document_title,document_link FROM article_document")
for i in cur.fetchall():
    titles.append(i)
for j in itertools.combinations(titles,2):
    cut_title1 = " ".join(jieba.cut(j[0][0], cut_all=False))
    cut_title2 = " ".join(jieba.cut(j[1][0], cut_all=False))
    a = [cut_title1,cut_title2]
    X = vectorizer.fit_transform(a)
    vec1 = list(X.toarray()[0])
    vec2 = list(X.toarray()[1])
    result = cosVector(vec1,vec2)
    if result>=0.25:
        print result
        print cut_title1,j[0][1]
        print cut_title2,j[1][1]
        print '-------------------------------------------------------'

conn.commit()
cur.close()
conn.close()


# In[12]:

import psycopg2
conn = psycopg2.connect("dbname='polldb' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()

a = [(945, 1206), (945, 1017), (935, 1168), (936, 1178), (936, 1179), (937, 1151), (937, 993), (938, 1183), (940, 1195), (940, 1010), (941, 1149), (941, 1191), (941, 1008), (942, 1004), (944, 1150), (944, 1182), (944, 1199), (944, 1011), (1038, 1177), (1041, 1165), (1042, 1192), (955, 1207), (955, 1013), (1144, 1170), (1146, 1162), (1146, 1173), (1146, 1189), (1146, 1200), (1146, 1006), (1147, 1194), (1148, 1168), (1149, 1191), (1149, 1008), (1150, 1152), (1150, 1190), (1150, 1199), (1150, 1005), (1150, 1011), (1151, 1182), (1152, 1225), (1152, 1011), (1154, 989), (1157, 990), (1161, 994), (1162, 1002), (1164, 1187), (1173, 996), (1174, 992), (1181, 1009), (1184, 998), (1185, 991), (1190, 1005), (1191, 1008), (1196, 1201), (1196, 1007), (1199, 1225), (1199, 1011), (1200, 1006), (1201, 1207), (1202, 1004), (1203, 1020), (1206, 1017), (1207, 1013), (1209, 1012), (1211, 1014), (1214, 1018)]
def combi(l):
    skip = []
    results = []
    for i in range(len(l)):
        if i in skip:
            continue
        result = l[i]
        for j in l[i]:
            for k in range(len(l)):
                if i==k:
                    continue
                if j in l[k]:
                    result += l[k]
                    skip.append(k)
        results.append(result)
    
    #刪除重複元素
    for a in range(len(results)):
        d = list(results[a])
        for b in results[a]:
            for c in range(d.count(b)-1):
                d.remove(b)
        results[a] = tuple(d)
    if results == l:
        return results
    else:
        return combi(results)
    
for o in combi(a):
    print o
    for pk in o:
        cur.execute('SELECT document_title,document_link FROM article_document WHERE document_pk=%s',(pk,))
        resu = cur.fetchall()
        print resu[0][0],resu[0][1]
    print '-------------------------------------------------'
'''
for o in combi(a):
    print o
print len(combi(a))
print type(combi(a))
'''


# In[ ]:

import requests
import json
import function

for k in function.keyword_extract('蔡英文明訪日　日本李登輝之友會協辦晚宴'):
    print k
res = requests.get('https://ajax.googleapis.com/ajax/services/search/web?v=1.0&q=的')
a = json.loads(res.text)
print a['responseData']['cursor']['estimatedResultCount']


# In[1]:

#兩文本相似度
#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from copy import deepcopy
# 作業系統
import os
import sys

# 字碼轉換
import codecs

# 科學運算
import numpy as np
import numpy.linalg as LA

# 文字處理
import nltk
from nltk.corpus import stopwords

# 移除中文停詞
def removeChineseStopWords(textFile):
    newTextFile = textFile

    chineseFilter1 = [u'，', u'。', u'、', u'；', u'：', u'？', u'「', u'」']

    for chin in chineseFilter1:
        newTextFile = newTextFile.replace(chin, ' ')
    
    return newTextFile
    
# 讀取中文檔案
def getTokensFromFile(textContent):
    for word in stopwords.words('english'):
        textContent = textContent.replace(word, ' ')
        
    textTokens = nltk.word_tokenize(removeChineseStopWords(textContent))
   
    return textTokens

# 字詞頻度表
def getTokenFreqList(textTokens):
    tokenFrequency = nltk.FreqDist(textTokens)

    # 刪除單一字
    k = deepcopy(tokenFrequency)
    for word in k:
        if len(word) == 1:
            tokenFrequency.pop(word)
    
    # 刪除數字
    k = deepcopy(tokenFrequency)
    for word in k:
        try:
            val = float(word)
            tokenFrequency.pop(word)
        except:
            pass
    
    # 刪除廢詞
    chineseFilter = [u'可能', u'不過', u'如果', u'恐怕', u'其實', u'進入', u'雖然', u'這麼',
                     u'處於', u'因為', u'一定']
    k = deepcopy(tokenFrequency)
    for word in k:
        if word in chineseFilter:
            tokenFrequency.pop(word)
    
    return tokenFrequency

# 計算 2 向量間距離
def getDocDistance(a, b):
    if LA.norm(a)==0 or LA.norm(b)==0:
        return -1
    
    return round(np.inner(a,b) / (LA.norm(a) * LA.norm(b)),4)
    
# 計算文件相似度    
def getDocSimilarity(wordFrequencyPair, minTimes=1):
    dict1 = {}
    for key in wordFrequencyPair[0].keys():
        if wordFrequencyPair[0].get(key, 0) >= minTimes:
            dict1[key] = wordFrequencyPair[0].get(key, 0)

    dict2 = {}
    for key in wordFrequencyPair[1].keys():
        if wordFrequencyPair[1].get(key, 0) >= minTimes:
            dict2[key] = wordFrequencyPair[1].get(key, 0)

    for key in dict2.keys():
        if dict1.get(key, 0) == 0:
            dict1[key] = 0
        
    for key in dict1.keys():
        if dict2.get(key, 0) == 0:
            dict2[key] = 0
        
    v1 = []
    for w in sorted(dict1.keys()):
        v1.append(dict1.get(w))
        #print "(1)", w, dict1.get(w)

    v2 = []    
    for w in sorted(dict2.keys()):
        v2.append(dict2.get(w))
        #print "(2)", w, dict2.get(w)

    result = 0
    
    try:
        result = getDocDistance(v1, v2)
    except(RuntimeError, TypeError, NameError):
        pass
        
    return result

def combi(l):
    skip = []
    results = []
    for i in range(len(l)):
        if i in skip:
            continue
        result = l[i]
        for j in l[i]:
            for k in range(len(l)):
                if i==k:
                    continue
                if j in l[k]:
                    result += l[k]
                    skip.append(k)
        results.append(result)
    
    #刪除重複元素
    for a in range(len(results)):
        d = list(results[a])
        for b in results[a]:
            for c in range(d.count(b)-1):
                d.remove(b)
        results[a] = tuple(d)
    if results == l:
        return results
    else:
        return combi(results)



from pymongo import MongoClient
import itertools
import jieba
import psycopg2

conn = psycopg2.connect("dbname='polldb_bak' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()

client = MongoClient()
db = client['poll_charlie']
collect1 = db['apple_news']
collect2 = db['udn_news']
collect3 = db['LTN_news']
collect4 = db['chinatimes_news']
collect5 = db['ptt_hatepolitic']
collect6 = db['match_news']

data = []
cur.execute("SELECT document_link,document_pk FROM article_document WHERE document_date BETWEEN '2015-11-24' AND '2015-11-25'")
for d in cur.fetchall():
    if collect1.find_one({'href':d[0]}) != None:
        doc = collect1.find_one({'href':d[0]})
    elif collect2.find_one({'href':d[0]}) != None:
        doc = collect2.find_one({'href':d[0]})
    elif collect3.find_one({'href':d[0]}) != None:
        doc = collect3.find_one({'href':d[0]})
    elif collect4.find_one({'href':d[0]}) != None:
        doc = collect4.find_one({'href':d[0]})
    elif collect5.find_one({'href':d[0]}) != None:
        doc = collect5.find_one({'href':d[0]})
    elif collect6.find_one({'href':d[0]}) != None:
        doc = collect6.find_one({'href':d[0]})
    else:
        print 'error',d[0]
        continue
    data.append((d[1],doc['href'],doc['content']))

pks = []
ind1 = 1
l = len(data)
t1 = l*(l-1)/2
for j in itertools.combinations(data,2):
    words1 = jieba.cut(j[0][2], cut_all=False)
    cut_content1 = " ".join(words1)
    words2 = jieba.cut(j[1][2], cut_all=False)
    cut_content2 = " ".join(words2)
    
    trainTokens = getTokensFromFile(cut_content1)
    trainTokenFrequency = getTokenFreqList(trainTokens)
    testTokens = getTokensFromFile(cut_content2)
    testTokenFrequency = getTokenFreqList(testTokens)
    wordFrequencyPair = [trainTokenFrequency, testTokenFrequency]
    value = getDocSimilarity(wordFrequencyPair, 1)
    if value>0.55:
        pks.append((j[0][0],j[1][0]))
        #print j[0][1]
        #print j[1][1]
        #print value
        #print '----------------------------------------'
    
    if ind1%1000 == 0:
        print '%d/%d'%(ind1,t1)
    ind1 += 1
    
for event in combi(pks):
    for doc_pk in event:
        cur.execute('SELECT document_title,document_link FROM article_document WHERE document_pk=%s',(doc_pk,))
        resu = cur.fetchone()
        print resu[0],resu[1]
    print '-------------------------------------------------'
print pks


# In[1]:

#兩文本相似度
#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from copy import deepcopy
# 作業系統
import os
import sys

# 字碼轉換
import codecs

# 科學運算
import numpy as np
import numpy.linalg as LA

# 文字處理
import nltk
from nltk.corpus import stopwords

# 移除中文停詞
def removeChineseStopWords(textFile):
    newTextFile = textFile

    chineseFilter1 = [u'，', u'。', u'、', u'；', u'：', u'？', u'「', u'」']

    for chin in chineseFilter1:
        newTextFile = newTextFile.replace(chin, ' ')
    
    return newTextFile
    
# 讀取中文檔案
def getTokensFromFile(textContent):
    for word in stopwords.words('english'):
        textContent = textContent.replace(word, ' ')
        
    textTokens = nltk.word_tokenize(removeChineseStopWords(textContent))
   
    return textTokens

# 字詞頻度表
def getTokenFreqList(textTokens):
    tokenFrequency = nltk.FreqDist(textTokens)

    # 刪除單一字
    k = deepcopy(tokenFrequency)
    for word in k:
        if len(word) == 1:
            tokenFrequency.pop(word)
    
    # 刪除數字
    k = deepcopy(tokenFrequency)
    for word in k:
        try:
            val = float(word)
            tokenFrequency.pop(word)
        except:
            pass
    
    # 刪除廢詞
    chineseFilter = [u'可能', u'不過', u'如果', u'恐怕', u'其實', u'進入', u'雖然', u'這麼',
                     u'處於', u'因為', u'一定']
    k = deepcopy(tokenFrequency)
    for word in k:
        if word in chineseFilter:
            tokenFrequency.pop(word)
    
    return tokenFrequency

# 計算 2 向量間距離
def getDocDistance(a, b):
    if LA.norm(a)==0 or LA.norm(b)==0:
        return -1
    
    return round(np.inner(a,b) / (LA.norm(a) * LA.norm(b)),4)
    
# 計算文件相似度    
def getDocSimilarity(wordFrequencyPair, minTimes=1):
    dict1 = {}
    for key in wordFrequencyPair[0].keys():
        if wordFrequencyPair[0].get(key, 0) >= minTimes:
            dict1[key] = wordFrequencyPair[0].get(key, 0)

    dict2 = {}
    for key in wordFrequencyPair[1].keys():
        if wordFrequencyPair[1].get(key, 0) >= minTimes:
            dict2[key] = wordFrequencyPair[1].get(key, 0)

    for key in dict2.keys():
        if dict1.get(key, 0) == 0:
            dict1[key] = 0
        
    for key in dict1.keys():
        if dict2.get(key, 0) == 0:
            dict2[key] = 0
        
    v1 = []
    for w in sorted(dict1.keys()):
        v1.append(dict1.get(w))
        #print "(1)", w, dict1.get(w)

    v2 = []    
    for w in sorted(dict2.keys()):
        v2.append(dict2.get(w))
        #print "(2)", w, dict2.get(w)

    result = 0
    
    try:
        result = getDocDistance(v1, v2)
    except(RuntimeError, TypeError, NameError):
        pass
        
    return result

def combi(l):
    skip = []
    results = []
    for i in range(len(l)):
        if i in skip:
            continue
        result = l[i]
        for j in l[i]:
            for k in range(len(l)):
                if i==k:
                    continue
                if j in l[k]:
                    result += l[k]
                    skip.append(k)
        results.append(result)
    
    #刪除重複元素
    for a in range(len(results)):
        d = list(results[a])
        for b in results[a]:
            for c in range(d.count(b)-1):
                d.remove(b)
        results[a] = tuple(d)
    if results == l:
        return results
    else:
        return combi(results)



from pymongo import MongoClient
import itertools
import jieba
import psycopg2

conn = psycopg2.connect("dbname='polldb_bak' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()

client = MongoClient()
db = client['poll_charlie']
collect1 = db['apple_news']
collect2 = db['udn_news']
collect3 = db['LTN_news']
collect4 = db['chinatimes_news']
collect5 = db['ptt_hatepolitic']
collect6 = db['match_news']

data = []
cur.execute("SELECT document_link,document_pk FROM article_document WHERE document_date BETWEEN '2015-11-24' AND '2015-11-25'")
for d in cur.fetchall():
    if collect1.find_one({'href':d[0]}) != None:
        doc = collect1.find_one({'href':d[0]})
    elif collect2.find_one({'href':d[0]}) != None:
        doc = collect2.find_one({'href':d[0]})
    elif collect3.find_one({'href':d[0]}) != None:
        doc = collect3.find_one({'href':d[0]})
    elif collect4.find_one({'href':d[0]}) != None:
        doc = collect4.find_one({'href':d[0]})
    elif collect5.find_one({'href':d[0]}) != None:
        doc = collect5.find_one({'href':d[0]})
    elif collect6.find_one({'href':d[0]}) != None:
        doc = collect6.find_one({'href':d[0]})
    else:
        print 'error',d[0]
        continue
    data.append((d[1],doc['href'],doc['content']))

pks = []
ind1 = 1
l = len(data)
t1 = l*(l-1)/2
for j in itertools.combinations(data,2):
    words1 = jieba.cut(j[0][2], cut_all=False)
    cut_content1 = " ".join(words1)
    words2 = jieba.cut(j[1][2], cut_all=False)
    cut_content2 = " ".join(words2)
    
    trainTokens = getTokensFromFile(cut_content1)
    trainTokenFrequency = getTokenFreqList(trainTokens)
    testTokens = getTokensFromFile(cut_content2)
    testTokenFrequency = getTokenFreqList(testTokens)
    wordFrequencyPair = [trainTokenFrequency, testTokenFrequency]
    value = getDocSimilarity(wordFrequencyPair, 1)
    if value>0.45:
        pks.append((j[0][0],j[1][0]))
        #print j[0][1]
        #print j[1][1]
        #print value
        #print '----------------------------------------'
    
    if ind1%1000 == 0:
        print '%d/%d'%(ind1,t1)
    ind1 += 1
    
for event in combi(pks):
    for doc_pk in event:
        cur.execute('SELECT document_title,document_link FROM article_document WHERE document_pk=%s',(doc_pk,))
        resu = cur.fetchone()
        print resu[0],resu[1]
    print '-------------------------------------------------'
print pks

