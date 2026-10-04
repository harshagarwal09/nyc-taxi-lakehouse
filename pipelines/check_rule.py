from functools import reduce
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("check_rule")
         .master("local[4]").config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

months = sorted(p.name.split("=")[1] for p in Path("data/bronze/yellow").glob("month=*"))

def load(m):
    return (spark.read.parquet(f"data/bronze/yellow/month={m}")
            .select(F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
                    F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
                    F.col("trip_distance").cast("double").alias("trip_distance"),
                    F.col("fare_amount").cast("double").alias("fare_amount"),
                    F.col("payment_type").cast("int").alias("payment_type"),
                    F.col("RatecodeID").cast("int").alias("ratecode"))
            .withColumn("file_month", F.lit(m)))

b = reduce(lambda x, y: x.unionByName(y), [load(m) for m in months])

dur = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60
passes_other_rules = (
    (F.date_format("pickup_ts", "yyyy-MM") == F.col("file_month"))
    & dur.between(1, 180)
    & F.col("trip_distance").between(0.1, 100)
    & (F.col("fare_amount") >= 3.0)
)
too_high = F.col("fare_amount") > 25 + 7 * F.col("trip_distance") + 1.4 * dur

only_high = b.filter(passes_other_rules & too_high).cache()
print("rows removed ONLY by fare_too_high:", only_high.count())

view = (only_high
        .withColumn("minutes", F.round(dur, 1))
        .withColumn("fare_per_mile", F.round(F.col("fare_amount") / F.col("trip_distance"), 1))
        .select("trip_distance", "minutes", "fare_amount", "fare_per_mile",
                "payment_type", "ratecode"))

print("20 RANDOM examples:")
view.orderBy(F.rand(7)).show(20)

print("BY PAYMENT TYPE:")
only_high.groupBy("payment_type").count().orderBy(F.desc("count")).show()

print("5 CLOSEST TO THE LIMIT (the most borderline removals):")
(view.withColumn("ceiling", F.round(25 + 7 * F.col("trip_distance") + 1.4 * F.col("minutes"), 1))
     .withColumn("over_by", F.round(F.col("fare_amount") - F.col("ceiling"), 1))
     .orderBy("over_by").show(5))

print("BY RATE CODE:")
only_high.groupBy("ratecode").count().orderBy(F.desc("count")).show()

print("rows with fare exactly 70.00:", only_high.filter(F.col("fare_amount") == 70.0).count())
print("fare 70.00 by rate code:")
only_high.filter(F.col("fare_amount") == 70.0).groupBy("ratecode").count().show()

spark.stop()