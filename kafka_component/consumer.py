from kafka import KafkaConsumer
import json

class TweetConsumer:
    def __init__(self, topic_name, bootstrap_servers='localhost:9092', start_from_beginning=True):
        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest' if start_from_beginning else 'latest'
        )

    def consume(self):
        for message in self.consumer:
            print(f"Received message: {message.value}")

class TickConsumer:
    def __init__(self, topic_name, bootstrap_servers='localhost:9092', start_from_beginning=True):
        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest' if start_from_beginning else 'latest',
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) # 反序列化消息
        )

    def consume(self):
        for message in self.consumer:
            print(f"Received message: {message.value}")