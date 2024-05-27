from pymongo import MongoClient
from kafka import KafkaProducer
from loguru import logger
import json
import time

# MongoDB 连接
mongo_uri = "mongodb+srv://fernando:Zz12345678@stockanalysis.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
client = MongoClient(mongo_uri)
db = client['stock_data']
collection = db['all_stocks_ticks']

# 创建索引以优化查询（如果尚未创建）
collection.create_index([('time', 1)])


# 定义数据读取和打印逻辑
def fetch_and_print_data():
    # 定义一个变量来保存上一次查询的最新时间戳
    last_timestamp = None

    while True:
        # 设置查询条件，仅获取比上一次查询更晚的数据
        query = {'time': {'$gt': last_timestamp}} if last_timestamp else {}
        # 查询数据并按时间戳排序
        cursor = collection.find(query).sort('time', 1)

        # 检查是否有新数据
        new_data_found = False
        for doc in cursor:
            print(doc)  # 打印每条数据
            last_timestamp = doc['time']  # 更新最后的时间戳
            new_data_found = True
            time.sleep(3)  # 每条数据打印后暂停3秒

        # 如果这次查询没有新数据，暂停一会再次查询
        if not new_data_found:
            time.sleep(10)  # 暂停10秒再次检查新数据

if __name__ == '__main__':
    fetch_and_print_data()