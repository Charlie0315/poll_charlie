
# coding: utf-8

# In[1]:

import psycopg2
import datetime

import numpy as np
import jieba
from pymongo import MongoClient

def cutSentence(text): ##放入原始文章路徑, 增加斷詞的list
    cutlist = u'<>/:：;；,、＂’，.。！？?「」｢\"\'\\\n\r《》“”!@#$%^&*() ' ##列出標點符號，並轉換成utf-8的格式
    textList = []
    sentence = ""
    for word in text:
        if word not in cutlist: #如果文字不是標點符號，就把字加到句子中
            sentence += word
            #print sentence
        else:
            if sentence == ' ':
                continue
            textList.append(sentence) #如果遇到標點符號，把句子加到 text list中
            sentence = ""
            #print textList
    if textList == []:
        return [text]
    return textList#傳回一個文字陣列

def match(word, sentiment_value):
    if word in mostdict:
        #print word+'(most)'
        sentiment_value *= 2.0
    elif word in verydict:
        #print word+'(very)'
        sentiment_value *= 1.5
    elif word in moredict:
        #print word+'(more)'
        sentiment_value *= 1.25
    elif word in ishdict:
        #print word+'(ish)'
        sentiment_value *= 0.5
    elif word in insufficientdict:
        #print word+'(insuff)'
        sentiment_value *= 0.25
    elif word in inversedict:
        #print word+'(inverse)'
        sentiment_value *= -1
    return sentiment_value

def transform_to_positive_num(poscount, negcount):
    pos_count = 0
    neg_count = 0
    if poscount < 0 and negcount >= 0:
        neg_count += negcount - poscount
        pos_count = 0
    elif negcount < 0 and poscount >= 0:
        pos_count = poscount - negcount
        neg_count = 0
    elif poscount < 0 and negcount < 0:
        neg_count = -poscount
        pos_count = -negcount
    else:
        pos_count = poscount
        neg_count = negcount
    return [pos_count, neg_count]

def sumup_sentence_sentiment_score(score_list):
    score_array = np.array(score_list) # Change list to a numpy array
    Pos = np.sum(score_array[:,0]) # Compute positive score
    Neg = np.sum(score_array[:,1])
    AvgPos = np.mean(score_array[:,0]) # Compute review positive average score, average score = score/sentence number
    AvgNeg = np.mean(score_array[:,1])
    StdPos = np.std(score_array[:,0]) # Compute review positive standard deviation score
    StdNeg = np.std(score_array[:,1])

    return [Pos, Neg, AvgPos, AvgNeg, StdPos, StdNeg]

def single_review_sentiment_score(review):
    single_review_senti_score = []
    cuted_review = cutSentence(review)

    for sent in cuted_review:
        seg_sent = list(jieba.cut(sent, cut_all=False))
        i = 0 # word position counter
        a = 0 # sentiment word position
        poscount = 0 # count a positive word
        negcount = 0 # count a negative word

        for word in seg_sent:
            if word in posdict:
                #print word+'(pos)'
                poscount += 1
                for w in seg_sent[a:i]:
                    poscount = match(w, poscount)
                a = i + 1
            elif word in negdict:
                #print word+'(neg)'
                negcount += 1
                for w in seg_sent[a:i]:
                    negcount = match(w, negcount)
                a = i + 1

            # Match "!" in the review, every "!" has a weight of +2
            elif word == "！".decode('utf8') or word == "!".decode('utf8'):
                for w2 in seg_sent[::-1]:
                    if w2 in posdict:
                        poscount += 2
                        break
                    elif w2 in negdict:
                        negcount += 2
                        break                    
            i += 1

        single_review_senti_score.append(transform_to_positive_num(poscount, negcount))
        review_sentiment_score = sumup_sentence_sentiment_score(single_review_senti_score)

    return review_sentiment_score

def get_txt_data(filepath):
    txt_data = []
    f = open(filepath, 'r')
    for i in f.readlines():
        txt_data.append(i.replace('\n','').decode('utf8'))
    f.close()
    return txt_data


conn = psycopg2.connect("dbname='polldb_bak' user='postgres' host='52.10.51.28' password='123456'")
cur = conn.cursor()

# Load sentiment dictionary
posdict = get_txt_data("C:/Users/Lenovo/Desktop/dict/posdict.txt")
negdict = get_txt_data("C:/Users/Lenovo/Desktop/dict/negdict.txt")

# Load adverbs of degree dictionary
mostdict = get_txt_data('C:/Users/Lenovo/Desktop/dict/most.txt')
verydict = get_txt_data('C:/Users/Lenovo/Desktop/dict/very.txt')
moredict = get_txt_data('C:/Users/Lenovo/Desktop/dict/more.txt')
ishdict = get_txt_data('C:/Users/Lenovo/Desktop/dict/ish.txt')
insufficientdict = get_txt_data('C:/Users/Lenovo/Desktop/dict/insufficiently.txt')
inversedict = get_txt_data('C:/Users/Lenovo/Desktop/dict/inverse.txt')

client = MongoClient()
db = client['poll_charlie']

#主程式
doc_pks = []
start_date = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d 00:00:00+08')
end_date = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d 23:59:59+08')
#start_date = '2015-12-12+08'
#end_date = '2015-12-13+08'
print start_date
print end_date
cur.execute("UPDATE article_candidate_allvalue SET newest=0 WHERE date BETWEEN %s AND %s",(start_date,end_date))
cur.execute("SELECT candidate_pk,keyword_fk_id FROM article_candidate ")
for i in cur.fetchall():
    comments_polarity = []
    doc_pks = []
    candidate_value = 0
    cur.execute("""SELECT D.document_value,D.source_fk_id,D.document_link
                    FROM article_document_join_keyword DJK,article_document D
                    WHERE DJK.document_fk_id=D.document_pk AND DJK.keyword_fk_id=%s
                    AND document_date BETWEEN %s AND %s """,(i[1],start_date,end_date))
    #計算候選人網路聲量
    for j in cur.fetchall():
        candidate_value += j[0]
        #計算候選人正負評
        try:
            if j[1] == 1:
                collect = db['apple_news']
                post = collect.find_one({'href':j[2]})
                for comment in post['comments']:
                    result = single_review_sentiment_score(comment['message'])
                    if result[0] > result[1]:
                        comments_polarity.append(1) #1:正評
                    #elif result[0] < result[1]:
                        #comments_polarity.append(-1)#-1:負評
                    #else:
                        #comments_polarity.append(0)#0:中立
                    else:
                        comments_polarity.append(-1)
            elif j[1] == 2:
                collect = db['udn_news']
                post = collect.find_one({'href':j[2]})
                for comment in post['comments']:
                    result = single_review_sentiment_score(comment['message'])
                    if result[0] > result[1]:
                        comments_polarity.append(1) #1:正評
                    #elif result[0] < result[1]:
                        #comments_polarity.append(-1)#-1:負評
                    #else:
                        #comments_polarity.append(0)#0:中立
                    else:
                        comments_polarity.append(-1)
                for comment in post['udn_comments']:
                    try:
                        result = single_review_sentiment_score(comment['message'])
                    except:
                        result = single_review_sentiment_score(comment['mesaage'])
                    if result[0] > result[1]:
                        comments_polarity.append(1) #1:正評
                    #elif result[0] < result[1]:
                        #comments_polarity.append(-1)#-1:負評
                    #else:
                        #comments_polarity.append(0)#0:中立
                    else:
                        comments_polarity.append(-1)
            elif j[1] == 3:
                collect = db['LTN_news']
                post = collect.find_one({'href':j[2]})
                for comment in post['comments']:
                    result = single_review_sentiment_score(comment['message'])
                    if result[0] > result[1]:
                        comments_polarity.append(1) #1:正評
                    #elif result[0] < result[1]:
                        #comments_polarity.append(-1)#-1:負評
                    #else:
                        #comments_polarity.append(0)#0:中立
                    else:
                        comments_polarity.append(-1)
            elif j[1] == 4:
                collect = db['chinatimes_news']
                post = collect.find_one({'href':j[2]})
                for comment in post['ct_comments']:
                    result = single_review_sentiment_score(comment['message'])
                    if result[0] > result[1]:
                        comments_polarity.append(1) #1:正評
                    #elif result[0] < result[1]:
                        #comments_polarity.append(-1)#-1:負評
                    #else:
                        #comments_polarity.append(0)#0:中立
                    else:
                        comments_polarity.append(-1)
            #elif j[1] == 5:
                #collect = db['ptt_hatepolitic']
            else:
                pass
        except Exception as e:
            print e
    t = len(comments_polarity)
    positive_value = comments_polarity.count(1)
    negative_value = comments_polarity.count(-1)

    print i[0]
    print positive_value
    print negative_value

    now_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    #now_time = '2015-12-12 22:28:30+08'
    cur.execute("""INSERT INTO article_candidate_allvalue(date,candidate_allvalue_value,candidate_allvalue_positve,candidate_allvalue_negative,candidate_fk_id,newest)
                    VALUES(%s,%s,%s,%s,%s,%s)""",(now_time,candidate_value,positive_value,negative_value,i[0],1))


conn.commit()
cur.close()
conn.close()
print 'success'

