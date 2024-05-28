from dotenv import load_dotenv
import pymongo
import os


CONNECTION_STRING = os.environ.get("COSMOS_CONNECTION_STRING")

class MongoDBConnector():
    def __init__(self, db_name, collection_name):
        load_dotenv()
        self.client = pymongo.MongoClient(CONNECTION_STRING)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def get_all_data(self, query):
        data = self.collection.find(query)
        return list(data)
    
    def write_row_in_mongo(df):
        df.write.format("mongo").mode("append").option("uri", CONNECTION_STRING).save()
