from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("kafka_to_bronze_batch_check") \
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.parquet("/data/silver/orders")
df.show(10)