from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.window import Window

try:
    with open("./checkpoint.txt") as f:
        last_version = int(f.read().strip())
except:
    last_version = 0

spark = SparkSession.builder \
        .appName("bronze_to_silver_batch_ingestion") \
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

read_path = "/data/bronze/orders"
Write_path = "/data/silver/orders"

# df = spark.read.parquet(read_path).select("data.*")
df = spark.read.format("delta") \
    .option("startingVersion", last_version+1) \
    .load("/data/bronze/orders")

df = df.select("data.*")

dedupedDf = df.dropDuplicates(["event_id"]).filter(("amount > 0 AND user_id IS NOT NULL"))

# this is the thing that we want to do in gold but done here only and skipping gold as it will be same as silver
window = Window.partitionBy(col("event_id")).orderBy(col("event_time").desc())

updatedDf = dedupedDf.withColumn("rn",row_number().over(window)).filter(col("rn") == 1).select("event_id","user_id","order_id","event_type","version","amount","status","event_time","category","payment_type")

history = spark.sql("DESCRIBE HISTORY delta.`/data/bronze/orders`")
new_version = history.selectExpr("max(version)").collect()[0][0]
with open("./checkpoint.txt", "w") as f:
    f.write(str(new_version))

updatedDf.write.format("delta").mode("append").save(Write_path)