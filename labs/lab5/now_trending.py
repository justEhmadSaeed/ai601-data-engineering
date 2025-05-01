# now_trending.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json, count, when, desc, current_timestamp, row_number
from pyspark.sql.functions import desc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql import SparkSession

# 1) Create SparkSession
spark = SparkSession.builder \
    .appName("NowTrendingSongs") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2) Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music_events") \
    .option("startingOffsets", "latest") \
    .load()

# 3) Parse the JSON 'value' from Kafka
schema = StructType([
    StructField("song_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("region", StringType(), True),
    StructField("action", StringType(), True)
])

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
events_df = parsed_df.select("data.*")

# 4) Filter and aggregate play, skip, like events
aggregated_df = events_df \
    .groupBy(
        window(current_timestamp(), "10 minutes"), # Change window here!
        col("region"),
        col("song_id")
    ) \
    .agg(
        count(when(col("action") == "play", 1)).alias("play_count"),
        count(when(col("action") == "skip", 1)).alias("skip_count"),
        count(when(col("action") == "like", 1)).alias("like_count")
    )

# 5) Compute skip ratio
skip_ratio_df = aggregated_df.withColumn(
    "skip_ratio",
    when(col("play_count") + col("skip_count") + col("like_count") > 0, col("skip_count") / (col("play_count") + col("skip_count") + col("like_count"))).otherwise(0)
)

# 6) Use foreachBatch to do rank-based top N and unpopular logic each micro-batch
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print("No data in this batch.")
        return

    w = Window.partitionBy("region", "window").orderBy(desc("play_count"))
    ranked_df = batch_df.withColumn("rn", row_number().over(w)).filter(col("rn") <= 3)
    unpopular_df = batch_df.filter(col("skip_ratio") > 0.5)

    print(f"=== Batch: {batch_id} ===")
    print("Top 3 Songs:")
    ranked_df.show(truncate=False)
    print("\nUnpopular Songs (Skip Ratio > 0.5):")
    unpopular_df.show(truncate=False)

# 7) Write Stream with foreachBatch and trigger
query = skip_ratio_df \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .trigger(processingTime='1 second') \
    .start()

query.awaitTermination()
