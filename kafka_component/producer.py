from kafka import KafkaProducer
from loguru import logger
from tqdm import tqdm
from mongodb_connector.connector import MongoDBConnector
import json
from utils.json_utils import json_serializer
import time
import random

class TweetListener:
    def __init__(self, mocker, producer, topic_name):
        self.mocker = mocker
        self.producer = producer
        self.topic_name = topic_name

    def process_tweet_data(self, tweet_data):
        if tweet_data['data']:
            data = {
                'message': tweet_data['data']['text'].replace(',', '')
            }
            self.producer.send(self.topic_name, value=json.dumps(data, ensure_ascii=False).encode('utf-8'))

    def start_processing_tweets(self, search_term, n=5):
        tweets_json = self.mocker.generate_tweets(search_term, n)
        for tweet in tweets_json:
            self.process_tweet_data(tweet)
            interval = random.randint(1, 5)
            time.sleep(interval)

class MockProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers
        )

    def send(self, topic, value):
        self.producer.send(topic, value)
        self.producer.flush()
        logger.info(f"Sending to {topic}: {value}")

class TickProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=json_serializer  # 使用自定义的JSON序列化函数
        )
        self.connector = MongoDBConnector(db_name="stock_data", collection_name="all_stocks_ticks")
    
    def send(self, topic):
        # 创建索引以优化查询（如果尚未创建）
        collection = self.connector.collection
        collection.create_index([('time', 1)])

        unique_timestamps = collection.distinct("time")
        unique_timestamps.sort()  # 确保按时间顺序处理
        
        for timestamp in tqdm(unique_timestamps, desc="Processing timestamps"):
            query = {"time": timestamp}
            records = collection.find(query)

            for record in records:
                # 发送数据到Kafka的特定主题
                self.producer.send(topic, record)
                self.producer.flush()