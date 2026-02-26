from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("BatchDataIngestion") \
        .getOrCreate()
    
    print("--- Spark Pipeline Started ---")
    # Logic Spark kamu akan ada di sini
    print("--- Spark Pipeline Finished ---")
    
    spark.stop()

if __name__ == "__main__":
    main()
