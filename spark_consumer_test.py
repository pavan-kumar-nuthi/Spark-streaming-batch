import json
from kafka import KafkaConsumer
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

consumer = KafkaConsumer('stream')  


def process_messages():
    for msg in consumer:
        print(json.loads(msg.value))


process_messages()
