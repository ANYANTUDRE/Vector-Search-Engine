import json
from pprint import pprint
import os
import time

from dotenv import load_dotenv
from elasticsearch import Elasticsearch

### reads & imports variables stored in .env file (environment variables)
load_dotenv()


class Search:
    def __init__(self):
        self.es = Elasticsearch('http://localhost:9200') 
        client_info = self.es.info()
        print('Connected to Elasticsearch!')
        pprint(client_info.body)


    def create_index(self):
        """This method create an index automatically
        deleting a previous instance of the index if it exists
        """
        
        self.es.indices.delete(index="my_docs", ignore_unavailable=True)
        self.es.indices.create(index="my_docs")


    def insert_document(self, doc):
        """This method inserts the doc into the my_docs index,
        returning the response from the service
        """
        return self.es.index(index="my_docs", body=doc)


    def insert_documents(self, documents):
        """This method assembles a single list of docs, and then passes the list to the bulk() method 
        which allows several operations to be communicated to the service in a single API call
        """
        operations = []
        for doc in documents:
            operations.append({"index": {"_index": "my_docs"}})
            operations.append(doc)
        return self.es.bulk(operations=operations)
    

    def reindex(self):
        self.create_index()
        with open('data.json', 'rt') as f:
            docs = json.loads(f.read())
        return self.insert_documents(documents=docs)
    
    
    def search(self, **query_args):
        """This method is used to submit a search query
        """
        return self.es.search(index="my_docs", **query_args)
    

    def retrieve_document(self, id):
        """This method render individual documents thanks to unique 
        identifiers that were assigned to each document
        """
        return self.es.get(index="my_docs", id=id)

    

    
