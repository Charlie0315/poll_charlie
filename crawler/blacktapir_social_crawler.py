
# coding: utf-8

# In[9]:


import pandas as pd
import requests
import psycopg2
from datetime import datetime

conn = psycopg2.connect("dbname='polldb_bak' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()

NAMES = [u"朱立倫", u"蔡英文", u"宋楚瑜", u"王如玄"]

for name in NAMES:
    url = "http://tag.analysis.tw/tag/%s/social/" % name

    r = requests.get(url)
    df = pd.read_html(r.text, header=0)[0]
    df.columns = ["time", "social", "news"]
    this_year = datetime.now().year
    df.time = pd.to_datetime(df.time.map(
        lambda x: "%s-%s" % (this_year, x)), format="%Y-%m-%d %H")
    df.time -= pd.Timedelta(hours=8)  # Force Convert to utc
    df["fetched"] = datetime.utcnow()
    df["name"] = name
    a = 0
    for record in df.to_dict(orient="records"):
        cur.execute("SELECT * FROM article_black_tapir_social WHERE kw_name=%s AND source_time=%s",(record['name'],record['time']))
        if cur.fetchone():
            break
        else:
            a += 1
            cur.execute("""INSERT INTO article_black_tapir_social
                        (kw_name,social,news,source_time,fetched_time)
                        VALUES(%s,%s,%s,%s,%s)""",(record['name'],record['social'],record['news'],record['time'],record['fetched']))
    
    print "%s has %s rows to insert" % (name.encode("utf8"), a)


conn.commit()
cur.close()
conn.close()
print 'success'

