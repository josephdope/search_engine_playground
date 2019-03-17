from tika import parser, language
import pymongo
from pymongo import MongoClient
import os
import datetime


class PDFPipeline():
    
    def __init__(self, mongo_connection_str, db):
        self.client = MongoClient(mongo_connection_str)
        self.db = self.client[db]
        
    def all_parse_to_db(self, directory, collection_name):
        collection = self.db[collection_name]
        agg = [{"$group" : {"_id" : 'null', "max": {"$max" : "$docID"}}}]
        print(list(collection.aggregate(agg)))
#        if dict(collection.aggregate(agg)) == None:
#            max_id = 0
#        else:
#            max_id = list(collection.aggregate(agg))
        for doc in os.listdir(directory):
            print(max_id)
            document = parser.from_file(directory+doc)
            content = document['content']
            meta = document['metadata']
            document = {**meta, 'content':content, 'timestamp':datetime.datetime.utcnow()}
            document_filtered = {k.replace(':',''):v for k,v in document.items() if '.' not in k}        
#            if collection.find(document_filtered).limit(1).count() == 0:
            collection.insert_one(document_filtered).inserted_id
#            else:
#                print('there was a problem entering this document into the database')
                


if __name__ == '__main__':
    
    connection_str = 'mongodb://localhost:27017/'
    pdf_directory = 'documents_raw/'
    pdf_pipeline = PDFPipeline(connection_str, 'tda_test')
    pdf_pipeline.all_parse_to_db(pdf_directory, 'documents')









#client = MongoClient('localhost', 27017)
#
#db = client['tda_test']
#collection = db.documents
#
#for doc in os.listdir('documents_raw'):
#    document = parser.from_file('documents_raw/'+doc)
#    content = document['content']
#    meta = document['metadata']
#    document = {**meta, 'content':content, 'timestamp':datetime.datetime.utcnow()}
#    document_filtered = {k:v for k,v in document.items() if '.' not in k}
#    collection.insert_one(document_filtered).inserted_id
    



