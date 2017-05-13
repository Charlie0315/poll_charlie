
# coding: utf-8

# In[7]:

import re
import requests
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime


def isfloat(string):
    return re.match("^\d+?\.\d+?$", string)

conn = psycopg2.connect("dbname='polldb_bak' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()

date = datetime.utcnow()

channels = {
    "fa-newspaper-o": "newspaper",
    "fa-minus": "unclear",
    "fa-facebook-square": "facebook"
}

urls = ['http://tag.analysis.tw/?order=burst','http://tag.analysis.tw/?order=index']
for url in urls:
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")

    rows = []

    for tr in soup.select("tr")[1:]:
        if isfloat(tr.select("td")[0].text):
            explosion, score, keyword, times, social = [
                td.text for td in tr.select("td")[:5]]
            channel = channels.get(
                tr.select("td")[5].find("i").attrs.get("class")[1])
            news_count = []
            for li in tr.select("td")[6].select("li"):
                src, count = li.find("img").attrs.get("src"),  li.find("span").text
                news_count.append([src, int(count)])
            rows.append({
                "date": date,
                "burst": float(explosion),
                "score": float(score),
                "kw_name": keyword,
                "times": int(times),
                "social": float(social),
                "channel": channel,
                "news_count": news_count
            })
    for row in rows:
        cur.execute("SELECT * FROM article_black_tapir_burst WHERE kw_name=%s",(row['kw_name'],))
        if cur.fetchone():
            print row['date']
            cur.execute("""UPDATE article_black_tapir_burst 
                            SET burst=%s,score=%s,times=%s,social=%s,channel=%s,date=%s
                            WHERE kw_name=%s""",(row['burst'],row['score'],row['times'],row['social'],row['channel'],row['date'],row['kw_name']))
        else:
            cur.execute("""INSERT INTO article_black_tapir_burst
                            (kw_name,burst,score,times,social,channel,date)
                            VALUES(%s,%s,%s,%s,%s,%s,%s)""",(row['kw_name'],row['burst'],row['score'],row['times'],row['social'],row['channel'],row['date']))
        res = requests.get("http://tag.analysis.tw/tag/%s/"%row['kw_name'])
        soup = BeautifulSoup(res.text)
        for i in soup.select('.timeList li'):
            title = re.findall('</a>(.*)<a class=\"like_href\"',str(i))[0]
            date = i.select('time')[0].text+'+08'
            link = i.select('.like_href')[0]['href']
            print link,row['kw_name']
            cur.execute("SELECT * FROM article_black_tapir_related_news WHERE link=%s AND tag=%s",(link,row['kw_name']))
            if not cur.fetchone():
                cur.execute("""INSERT INTO article_black_tapir_related_news
                                (link,title,date,tag) VALUES(%s,%s,%s,%s)"""
                                ,(link,title,date,row['kw_name']))
                
conn.commit()
cur.close()
conn.close()
print 'success'


# In[6]:

import psycopg2
conn = psycopg2.connect("dbname='polldb_bak' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()
cur.execute("SELECT * FROM article_black_tapir_burst WHERE kw_name=%s",('蔡英文',))
print cur.fetchone()
cur.close()
conn.close()

