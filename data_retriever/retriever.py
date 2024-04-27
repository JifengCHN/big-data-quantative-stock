from dotenv import load_dotenv
import pymongo
import os

load_dotenv()
CONNECTION_STRING = os.environ.get("COSMOS_CONNECTION_STRING")

class Retriever():
    def __init__(self, db_name, collection_name):
        self.client = pymongo.MongoClient(CONNECTION_STRING)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def get_all_data(self, query):
        data = self.collection.find(query)
        return list(data)
