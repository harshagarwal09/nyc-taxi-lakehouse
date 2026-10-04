from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("check_silver")
         .master("local[4]").config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/silver/yellow")
df.printSchema()

print("rows per month:")
df.groupBy("file_month").count().orderBy("file_month").show()

print("pickup range:")
df.select(F.min("pickup_ts"), F.max("pickup_ts")).show(truncate=False)

print("value ranges:")
df.select(F.min("trip_distance"), F.max("trip_distance"),
          F.min("fare_amount"), F.max("fare_amount")).show()
spark.stop()