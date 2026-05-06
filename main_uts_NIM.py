# MAIN UTS BIG DATA (STABLE)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, window, sum as _sum, hour
import random
from datetime import datetime, timedelta
import os
import shutil

# 1. Mendapatkan Absolute Path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# 2. Init Spark
spark = SparkSession.builder \
    .appName("UTS_BigData_Processing") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("🚀 Spark Ready... Memulai Pemrosesan...")

# 3. Prepare Output Folder
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 4. Generate Dummy Data[cite: 1]
locations = ["AreaA", "AreaB", "AreaC"]
start_time = datetime(2026, 1, 1, 7, 0)
sensor_data = []

for i in range(100):
    for loc in locations:
        sensor_data.append((
            start_time + timedelta(minutes=i),
            loc,
            random.randint(10, 100)
        ))

sensor_df = spark.createDataFrame(sensor_data, ["timestamp", "location", "vehicle_count"])

# 5. Processing Logic[cite: 1]
# Total kendaraan per area
traffic_df = sensor_df.groupBy("location") \
    .agg(_sum("vehicle_count").alias("total_vehicle"))

# Tren per 10 menit
traffic_time_df = sensor_df.groupBy(
    window(col("timestamp"), "10 minutes"),
    "location"
).agg(_sum("vehicle_count").alias("total_vehicle"))

# Data untuk AI
ml_df = sensor_df.withColumn("hour", hour(col("timestamp")))

# 6. Save to Parquet[cite: 1]
def save_data(df, folder_name):
    try:
        path = os.path.join(OUTPUT_DIR, folder_name)
        print(f"📂 Menyimpan data ke: {path}...")
        df.write.mode("overwrite").parquet(path)
    except Exception as e:
        print(f"\n❌ ERROR SAAT MENULIS DATA: {str(e)}")

save_data(traffic_df, "traffic")
save_data(traffic_time_df, "traffic_time")
save_data(ml_df, "ml_data")

print("\n✅ SEMUA DATA BERHASIL DISIMPAN KE FOLDER OUTPUT")

# 7. Close Session[cite: 1]
spark.stop()
print("🔌 Spark Session Closed. Silakan jalankan Streamlit sekarang.")