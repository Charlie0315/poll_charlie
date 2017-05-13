
# coding: utf-8

# In[1]:

import requests
import psycopg2
from bs4 import BeautifulSoup

def insert_mediapoll(date,candidate_fk_id,source_fk_id,media_poll_value):
    cur.execute("""INSERT INTO article_mediapoll(date,candidate_fk_id,source_fk_id,media_poll_value)
                    VALUES (%s,%s,%s,%s)""",(date,candidate_fk_id,source_fk_id,media_poll_value))
    conn.commit()

def get_source_pk(source_name):
    cur.execute("SELECT source_pk FROM article_source WHERE source_name=%s",(source_name,))
    result = cur.fetchall()
    if result == []:
        cur.execute("INSERT INTO article_source(source_name,source_html,big_source_fk_id) VALUES(%s,%s,%s)",(source_name,'',4))
        conn.commit()
        return get_source_pk(source_name)
    else:
        return result[0][0]

conn = psycopg2.connect("dbname='polldb_bak' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()

res = requests.get('https://zh.wikipedia.org/wiki/2016%E5%B9%B4%E4%B8%AD%E8%8F%AF%E6%B0%91%E5%9C%8B%E7%B8%BD%E7%B5%B1%E9%81%B8%E8%88%89')
soup = BeautifulSoup(res.text)

for entry in soup.select('.wikitable.collapsible')[24].select('tr')[3:]:
    try:
        source = entry.select('td')[0].text
    except Exception as e:
        continue
    if u'*' in source:
        source = source.replace(u'*',u'')
    if u'／' in source:
        source = source.split(u'／')[0]
    if u'-' in source:
        source = source.split(u'-')[0]
    if u'[' in source:
        source = source.split(u'[')[0]
    print source
    source_pk = get_source_pk(source)
    date = entry.select('td')[1].text.replace(u'年',u'-')
    date = date.replace(u'月',u'-')
    date = date.replace(u'日',u'')
    #如果已經有資料就跳過
    cur.execute('SELECT * FROM article_mediapoll WHERE date=%s AND source_fk_id=%s',(date,source_pk))
    if cur.fetchall() != []:
        continue
    try:
        tsai_value = entry.select('td')[2].text.replace(u'%',u'')
        print date,1,source_pk,tsai_value
        insert_mediapoll(date,1,source_pk,tsai_value)
        chu_value = entry.select('td')[3].text.replace(u'%',u'')
        print date,4,source_pk,chu_value
        insert_mediapoll(date,4,source_pk,chu_value)
        soong_value = entry.select('td')[4].text.replace(u'%',u'')
        print date,3,source_pk,soong_value
        insert_mediapoll(date,3,source_pk,soong_value)
    except Exception as e:
        continue

cur.close()
conn.close()
print 'success'

