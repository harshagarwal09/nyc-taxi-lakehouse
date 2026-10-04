from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("explore_fares")
         .master("local[4]").config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

s = spark.read.parquet("data/silver/yellow")
s = s.withColumn("fare_per_mile", F.col("fare_amount") / F.col("trip_distance"))

print("FARE percentiles (0.1%, 1%, 50%, 99%, 99.9%, 99.99%) and max:")
s.selectExpr(
    "percentile_approx(fare_amount, array(0.001, 0.01, 0.5, 0.99, 0.999, 0.9999)) as fare_pcts",
    "max(fare_amount) as max_fare").show(truncate=False)

print("FARE PER MILE percentiles and max:")
s.selectExpr(
    "percentile_approx(fare_per_mile, array(0.001, 0.01, 0.5, 0.99, 0.999, 0.9999)) as fpm_pcts",
    "max(fare_per_mile) as max_fpm").show(truncate=False)

print("CHEAP FARES (rows):")
s.select(F.sum((F.col("fare_amount") < 1).cast("int")).alias("under_1"),
         F.sum((F.col("fare_amount") < 3).cast("int")).alias("under_3"),
         F.sum((F.col("fare_amount") < 5).cast("int")).alias("under_5")).show()

print("EXPENSIVE FARES, count >= 300:", s.filter(F.col("fare_amount") >= 300).count())
s.filter(F.col("fare_amount") >= 300) \
 .select("pickup_ts", "trip_distance", "fare_amount", "total_amount", "payment_type") \
 .orderBy(F.desc("fare_amount")).show(15, truncate=False)

print("TIPS BIGGER THAN THE FARE, count:", s.filter(F.col("tip_amount") > F.col("fare_amount")).count())
spark.stop()