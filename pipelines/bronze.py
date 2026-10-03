from pathlib import Path
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("bronze")
         .master("local[4]").config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

for f in sorted(Path("data/raw").glob("yellow_tripdata_*.parquet")):
    month = f.stem.replace("yellow_tripdata_", "")
    df = (spark.read.parquet(str(f))
          .withColumn("source_file", F.lit(f.name))
          .withColumn("ingested_at", F.current_timestamp()))
    df.write.mode("overwrite").parquet(f"data/bronze/yellow/month={month}")
    print("bronze", month, df.count(), "rows")

spark.stop()