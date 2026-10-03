from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("explore")
         .master("local[4]").config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/raw/yellow_tripdata_2024-01.parquet")

df.printSchema()
print("rows:", df.count())
df.show(5)

print("earliest and latest pickup:")
df.select(F.min("tpep_pickup_datetime"), F.max("tpep_pickup_datetime")).show(truncate=False)

print("distance <= 0:", df.filter(F.col("trip_distance") <= 0).count())
print("fare < 0:", df.filter(F.col("fare_amount") < 0).count())
print("dropoff before pickup:",
      df.filter(F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime")).count())