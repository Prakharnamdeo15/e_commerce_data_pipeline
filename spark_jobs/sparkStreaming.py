from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json, window, sum, count, approx_count_distinct
from pyspark.sql.types import *



spark = SparkSession.builder.appName("realtime_dashboard").config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers","kafka:9092") \
     .option("subscribe","order_events").option("startingOffsets","latest").load()

schema = StructType([
    StructField("event_id",StringType()),
    StructField("user_id",StringType()),
    StructField("order_id",StringType()),
    StructField("event_type", StringType()),
    StructField("version",IntegerType()),
    StructField("amount",DoubleType()),
    StructField("status",StringType()),
    StructField("event_time",TimestampType()),
    StructField("category",StringType()),
    StructField("payment_type",StringType())
])

parsedDf = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

stream_df = parsedDf.withWatermark("event_time","10 minutes")
stream_df = stream_df.dropDuplicates(["event_id"])

metrics_5min = stream_df.groupBy(
    window("event_time", "5 minutes")
).agg(
    sum("amount").alias("total_amount"),
    approx_count_distinct("user_id").alias("active_users")
)

query = metrics_5min.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation","/data/checkpoints/revenue") \
    .start("/data/gold/revenue_5min")


query.awaitTermination()
