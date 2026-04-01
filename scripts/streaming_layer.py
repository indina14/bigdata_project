from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("StreamingPipeline") \
    .getOrCreate()

# Mengatur log level untuk mengurangi noise di terminal
spark.sparkContext.setLogLevel("ERROR")

# Mendefinisikan schema data transaksi
schema = "user_id INT, product STRING, price DOUBLE, city STRING, timestamp STRING"

# Membaca data streaming dari folder 'stream_data' dalam format JSON
stream_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .json("stream_data")

# Menulis hasil streaming ke dalam format Parquet di folder serving layer
query = stream_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/serving/stream") \
    .option("checkpointLocation", "logs/stream_checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start()

# Menunggu terminasi proses streaming
query.awaitTermination()