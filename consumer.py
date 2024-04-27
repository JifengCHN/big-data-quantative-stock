from kafka import KafkaConsumer
import json

# Kafka 消费者设置
consumer = KafkaConsumer(
    'stock_prices',  # Kafka主题
    bootstrap_servers=['localhost:9092'],  # Kafka集群地址
    auto_offset_reset='latest',  # 从最新的消息开始读取
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # 消息反序列化
)

# 打印接收到的消息
for message in consumer:
    print(message.value)
