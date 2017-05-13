
# coding: utf-8

# In[1]:

import psycopg2

conn = psycopg2.connect("dbname='polldb' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()
Wfbc = 2
Wfbs = 4

#def calculate_chinatimes():
    
def calculate_apple():
    cur.execute("SELECT document_pk,document_like,document_message_number FROM article_document WHERE source_fk_id=1")
    for doc in cur.fetchall():
        doc_value = doc[1]+doc[2]*Wfbc
        cur.execute("UPDATE article_document SET document_value=%s WHERE document_pk=%s",(doc_value,doc[0]))
    conn.commit()
    
def calculate_udn():
    cur.execute("SELECT document_pk,document_like,document_message_number FROM article_document WHERE source_fk_id=2")
    for doc in cur.fetchall():
        doc_value = doc[1]+doc[2]*Wfbc
        cur.execute("UPDATE article_document SET document_value=%s WHERE document_pk=%s",(doc_value,doc[0]))
    conn.commit()
    
def calculate_LTN():
    cur.execute("SELECT document_pk,document_like,document_message_number FROM article_document WHERE source_fk_id=3")
    for doc in cur.fetchall():
        doc_value = doc[1]+doc[2]*Wfbc
        cur.execute("UPDATE article_document SET document_value=%s WHERE document_pk=%s",(doc_value,doc[0]))
    conn.commit()
    
def calculate_ptt_hatepolitic():
    cur.execute("SELECT document_pk,document_message_number FROM article_document WHERE source_fk_id=5")
    for doc in cur.fetchall():
        doc_value = doc[1]
        cur.execute("UPDATE article_document SET document_value=%s WHERE document_pk=%s",(doc_value,doc[0]))
    conn.commit()
    
def calculte_fb_fanpage():
    cur.execute("SELECT document_pk,document_like,document_share,document_message_number FROM article_document WHERE source_fk_id=18")
    for doc in cur.fetchall():
        doc_value = doc[1]+doc[2]*Wfbs+doc[3]*Wfbc
        cur.execute("UPDATE article_document SET document_value=%s WHERE document_pk=%s",(doc_value,doc[0]))
    conn.commit()
    
calculate_apple()
calculate_udn()
calculate_LTN()
calculate_ptt_hatepolitic()
calculte_fb_fanpage()
cur.close()
conn.close()

