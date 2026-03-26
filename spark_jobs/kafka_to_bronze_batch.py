from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("kafka_to_bronze_batch_ingestion") \
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:9092")\
                               .option("subscribe", "order_events")\
                               .option("startingOffsets", "earliest").load()

schema = StructType([
    StructField("event_id",StringType()),
    StructField("user_id",StringType()),
    StructField("order_id",StringType()),
    StructField("event_type",StringType()),
    StructField("version",IntegerType()),
    StructField("amount",DoubleType()),
    StructField("status",StringType()),
    StructField("event_time",TimestampType()),
    StructField("category",StringType()),
    StructField("payment_type",StringType())
])

parsed_df = df.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data"), "timestamp", "partition", "offset" 
)
# fdf = parsed_df.select("data.*","partition","offset")


parsed_df.write \
    .format("delta") \
    .mode("append") \
    .save("/data/bronze/orders")
