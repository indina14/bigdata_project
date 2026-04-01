from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Inisialisasi Spark
spark = SparkSession.builder \
    .appName("TransportationStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# SCHEMA HARUS LENGKAP (Termasuk fare dan vehicle_type)
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("speed", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("fare", IntegerType(), True),         # <--- WAJIB ADA
    StructField("vehicle_type", StringType(), True),  # <--- WAJIB ADA
    StructField("timestamp", StringType(), True)
])

# 1. Read Stream
transport_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .json("data/raw/transportation")

# 2. Add Alert Column (Sederhana saja)
processed_df = transport_df.withColumn(
    "alert", 
    when(col("speed") < 20, "Macet").otherwise("Normal")
)

# 3. Write Stream ke Serving Layer
query = processed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/serving/transportation") \
    .option("checkpointLocation", "logs/transport_checkpoint") \
    .start()

print("Streaming Transportation Layer is Running... (Menunggu data dari Generator)")
query.awaitTermination()