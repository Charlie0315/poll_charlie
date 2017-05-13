
# coding: utf-8

# In[5]:


import requests
from datetime import datetime
from pymongo import MongoClient

client = MongoClient()
db = client["poll_charlie"]


urls = {
    "distance_tags": "http://tag.analysis.tw/api/distance.php?tag=%E7%8E%8B%E5%A6%82%E7%8E%84",
    "news_tags": "http://tag.analysis.tw/api/aiib.php?tag=%E7%8E%8B%E5%A6%82%E7%8E%84"
}

for name, url in urls.iteritems():
    response = requests.get(url)
    d = datetime.utcnow()
    collection = db[name]
    doc = {"fetched": datetime.utcnow(), "data": response.json()}
    print len(doc['data'])
    collection.insert(doc)

