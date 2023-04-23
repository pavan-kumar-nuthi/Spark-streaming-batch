from pydoc_data.topics import topics
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.sql import *


from pyspark.sql.functions import *

# import json_schema
scala_version = "2.12"
spark_version = "3.4.0"
# TODO: Ensure match above values match the correct versions
packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
    "org.apache.kafka:kafka-clients:3.4.0",
]

spark = (
    SparkSession.builder.master("local")
    .appName("kafka-example")
    .config("spark.jars.packages", ",".join(packages))
    .config("spark.jars", 'mysql-connector-java-5.1.48.jar')
    .getOrCreate()
)

# topics_names = ["BTC-USD", "ETH-USD"]
# spark = SparkSession.builder.appName('sample').getOrCreate()
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "BTC-USD,ETH-USD")
    .option("startingOffsets", "earliest")
    .load()
)

print(kafka_df)
json_schema = (
    StructType()
    .add("price", StringType(), False)
    .add("time", StringType(), False)
    .add("volume_24h", StringType(), False)
    .add("product_id", StringType(), False)
    .add("best_bid", StringType(), False)
    .add("best_ask", StringType(), False)
)
# Parse JSON data and extract relevant fields
parsed_df = (
    kafka_df.select(col("value").cast("string").alias("data"))
    .withColumn("jsonData", from_json(col("data"), json_schema, {"mode": "PERMISSIVE"}))
    .select("jsonData.*")
    .select(
        col("price").cast("double"),
        col("best_bid").cast("double"),
        col("best_ask").cast("double"),
        col("product_id"),
        col("time"),
        col("volume_24h").cast("double"),
    )
)

parsed_df.printSchema()

# Convert timestamp to Spark timestamp format
processed_df = parsed_df.withColumn(
    "timestamp", to_utc_timestamp(substring(col("time"), 1, 19), "UTC")
)

processed_df.printSchema()

# Group by base, window and aggregate
processed_df = processed_df.withWatermark("timestamp", "10 minutes")
window_duration = "1 minute"
aggregated_df = (
    processed_df.groupBy(col("product_id"), window(col("timestamp"), window_duration))
    .agg(
        {
            "price": "mean",
            "volume_24h": "mean",
            "timestamp": "max",
            "best_bid": "max",
            "best_ask": "max",
        }
    )
    .select(
        "product_id",
        "window.start",
        "window.end",
        "avg(price)",
        "avg(volume_24h)",
        "max(timestamp)",
        "max(best_ask)",
        "max(best_bid)",
    )
)

aggregated_df.printSchema()

aggregated_df = (
    aggregated_df.withColumnRenamed("max(timestamp)", "time")
    .withColumnRenamed("avg(price)", "price")
    .withColumnRenamed("max(best_bid)", "best_bid")
    .withColumnRenamed("max(best_ask)", "best_ask")
    .withColumnRenamed("avg(volume_24h)", "volume_24h")
)

aggregated_df.printSchema()

db = {"user": "root", "password": ""}


def tomysql(df, epoch_id):
    dfwriter = df.write.mode("append")
    dfwriter.jdbc(url='jdbc:mysql://localhost:3306/batch',
                  table='spark_agg', properties=db)

query = aggregated_df.writeStream.outputMode("append").foreachBatch(tomysql).start()

query1 = (
    aggregated_df.selectExpr("to_json(struct(*)) AS value")
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "./checkpoint")
    .option("topic", "stream")
    .start()
)


query1.awaitTermination()
query.awaitTermination()
