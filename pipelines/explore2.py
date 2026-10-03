from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("explore2")
         .master("local[4]").config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/raw/yellow_tripdata_2024-01.parquet")

# 1) How many pickups fall outside January 2024?
outside = df.filter(F.date_format("tpep_pickup_datetime", "yyyy-MM") != "2024-01").count()
print("pickups outside Jan 2024:", outside)

# 2) Trip duration in minutes
df = df.withColumn(
    "duration_min",
    (F.unix_timestamp(F.col("tpep_dropoff_datetime").cast("timestamp"))
     - F.unix_timestamp(F.col("tpep_pickup_datetime").cast("timestamp"))) / 60)

# 3) Typical vs extreme values (percentiles + max)
df.selectExpr(
    "percentile_approx(trip_distance, array(0.5, 0.99, 0.999)) as distance_p50_p99_p999",
    "max(trip_distance) as max_distance",
    "percentile_approx(duration_min, array(0.5, 0.99, 0.999)) as duration_p50_p99_p999",
    "max(duration_min) as max_duration_min",
    "max(fare_amount) as max_fare",
).show(truncate=False)

# 4) Payment types
df.groupBy("payment_type").count().orderBy(F.desc("count")).show()