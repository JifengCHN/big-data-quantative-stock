from kafka import KafkaConsumer

class TweetConsumer:
    def __init__(self, topic_name, bootstrap_servers='localhost:9092'):
        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest' # 从主题的开始位置开始消费
        )

    def consume(self):
        for message in self.consumer:
            print(f"Received message: {message.value}")