import pymongo
from pymongo import MongoClient
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer 
from collections import defaultdict
from nltk.corpus import stopwords

mongo_connection_str = 'mongodb://localhost:27017/'

client = MongoClient(mongo_connection_str)

db = client['tda_test']

documents = db.documents.find({}, {'content':1, '_id':0})

set(stopwords.words('english'))

def my_tokenizer(doc):
    lemmatizer = WordNetLemmatizer()
    tokenizer = RegexpTokenizer(r'\w+')
    article_tokens = tokenizer.tokenize(doc.lower())
    return article_tokens

doc_count = 0
doc_tokens = {}
for doc in documents:
    try:
        doc_tokens[doc_count] = my_tokenizer(doc['content'])
    except:
        print('document '+str(doc_count)+' returned no content')
    doc_count += 1

    word_dict = set(lemmatizer.lemmatize(word) for word in article_tokens if word not in set(stopwords.words('english'))