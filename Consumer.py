from pydoc_data.topics import topics
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.sql import *
# from pyspark.sql.functions import json_schema
from pyspark.sql.functions import *
# import json_schema
scala_version = '2.12'
spark_version = '3.3.1'
# TODO: Ensure match above values match the correct versions
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.4.0'
]

spark = SparkSession.builder\
   .master("local")\
   .appName("kafka-example")\
   .config("spark.jars.packages", ",".join(packages))\
   .getOrCreate()
topics_names=['BTC-USD','ETH-USD']
# spark = SparkSession.builder.appName('sample').getOrCreate()
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", ",".join(topics_names)) \
    .option("startingOffsets", "earliest") \
    .load()

print(kafka_df)
json_schema=StructType([
    StructField("price",DoubleType(),True),
    StructField("base",StringType(),True),
    StructField("volume",DoubleType(),True),
    StructField("timestamp",StringType(),True)
])
# Parse JSON data and extract relevant fields
parsed_df = kafka_df \
    .select(from_json(col("value").cast("string"),json_schema).alias("data")) \
    .selectExpr("data.price", "data.base", "data.volume","data.timestamp")

# Convert timestamp to Spark timestamp format
processed_df = parsed_df \
    .withColumn("timestamp", to_timestamp(col("timestamp") / 1000)) \
    .withWatermark("timestamp", "1 minute")

# window_duration='120'
# window_duration='10 second'
# win
window_duration='1 minute'
# Group by base, window and aggregate
aggregated_df = processed_df \
    .groupBy(col("base"), window(col("timestamp"), window_duration)) \
    .agg({"price": "mean", "timestamp": "max", "volume":"sum"})


# Print processed data on the console
processed_query = aggregated_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Start the query and wait for it to terminate
processed_query.awaitTermination()