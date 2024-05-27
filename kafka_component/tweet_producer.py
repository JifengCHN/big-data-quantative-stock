from kafka import KafkaProducer
from loguru import logger
import json
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
            self.producer.send(self.topic_name, value=json.dumps(data).encode('utf-8'))

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
