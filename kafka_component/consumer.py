from kafka import KafkaConsumer
import json
import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from mongodb_connector.connector import write_row_in_mongo
from pyspark.sql.functions import from_json, udf
import re
import os
from dotenv import load_dotenv

class TweetConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        COSMOS_CONNECTION_STRING = os.environ.get("COSMOS_CONNECTION_STRING")
        self.bootstrap_servers = bootstrap_servers
        findspark.init()
        self.spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("TwitterSentimentAnalysis") \
            .config("spark.mongodb.input.uri", COSMOS_CONNECTION_STRING) \
            .config("spark.mongodb.output.uri", COSMOS_CONNECTION_STRING) \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .getOrCreate()

    def print_message(self, topic_name, start_from_beginning=True):
        self.spark.conf.set("spark.sql.shuffle.partitions", "5")
        self.spark.conf.set("spark.sql.streaming.numRows", "100")
        self.spark.conf.set("spark.sql.debug.maxToStringFields", "100")

        # 从 Kafka 读取数据并转换格式
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest" if start_from_beginning else "latest") \
            .option("header", "true") \
            .load() \
            .selectExpr("CAST(value AS STRING) as message")
        
        schema = StructType([StructField("message", StringType())])
        df = df \
            .withColumn("value", from_json("message", schema))

        # 转换为小写\移除 URLs\移除非字母或非中文字符\清理开始和结束的空格，以及多余的空格\分割字符串为单词数组
        df = df.withColumn("lower_message", F.lower(F.col("value.message")))
        df = df.withColumn("no_urls", F.regexp_replace(F.col("lower_message"), "http\\S+|www.\\S+", ""))
        df = df.withColumn("only_alpha_or_chinese", F.regexp_replace(F.col("no_urls"), "[^a-zA-Z\\s\\u4e00-\\u9fff]", ""))
        df = df.withColumn("trimmed", F.trim(F.regexp_replace(F.col("only_alpha_or_chinese"), "\\s+", " ")))
        df = df.withColumn("cleaned_data", F.split(F.col("trimmed"), " "))
        df = df.filter(F.size(F.col("cleaned_data")) > 0)

        # 在控制台输出当前的 DataFrame
        df.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", False) \
            .start() \
            .awaitTermination()

        
    def consume(self, topic_name, path_to_model, start_from_beginning=True):
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest" if start_from_beginning else "latest") \
            .option("header", "true") \
            .load() \
            .selectExpr("CAST(value AS STRING) as message")

        schema = StructType([StructField("message", StringType())])
        df = df \
            .withColumn("value", from_json("message", schema))
        
        # 转换为小写\移除 URLs\移除非字母或非中文字符\清理开始和结束的空格，以及多余的空格\分割字符串为单词数组
        df = df.withColumn("lower_message", F.lower(F.col("value.message")))
        df = df.withColumn("no_urls", F.regexp_replace(F.col("lower_message"), "http\\S+|www.\\S+", ""))
        df = df.withColumn("only_alpha_or_chinese", F.regexp_replace(F.col("no_urls"), "[^a-zA-Z\\s\\u4e00-\\u9fff]", ""))
        df = df.withColumn("trimmed", F.trim(F.regexp_replace(F.col("only_alpha_or_chinese"), "\\s+", " ")))
        df = df.withColumn("cleaned_data", F.split(F.col("trimmed"), " "))
        df = df.filter(F.size(F.col("cleaned_data")) > 0)

        pipeline_model = PipelineModel.load(path_to_model)
        prediction = pipeline_model.transform(df)
        prediction = prediction.select(prediction.message, prediction.prediction)
        prediction \
            .writeStream \
            .format("console") \
            .outputMode("update") \
            .option("truncate", False) \
            .start() \
            .awaitTermination()
        
        query = prediction.writeStream.queryName("tweets") \
            .foreachBatch(write_row_in_mongo) \
            .start() \
            .awaitTermination()
        

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