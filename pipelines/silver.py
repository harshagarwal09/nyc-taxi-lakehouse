from functools import reduce
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("silver")
         .master("local[4]").config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

months = sorted(p.name.split("=")[1] for p in Path("data/bronze/yellow").glob("month=*"))

def load_month(m):
    df = spark.read.parquet(f"data/bronze/yellow/month={m}")
    return df.select(
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("int").alias("ratecode"),
        F.col("PULocationID").cast("int").alias("pu_zone"),
        F.col("DOLocationID").cast("int").alias("do_zone"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
    ).withColumn("file_month", F.lit(m))

raw = reduce(lambda a, b: a.unionByName(b), [load_month(m) for m in months])

def bad(cond):
    return F.coalesce(cond, F.lit(True))   # a NULL result counts as bad

duration_min = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60

# Highest fare a meter could plausibly produce for this trip
metered_ceiling = 25 + 7 * F.col("trip_distance") + 1.4 * duration_min

# Flat/negotiated fares (rate code 2 = JFK flat, 5 = negotiated) don't depend on
# distance, so they get a ceiling of at least $300 instead.
flat_codes = F.col("ratecode").isin(2, 5)
max_fare = F.when(flat_codes, F.greatest(metered_ceiling, F.lit(300.0))).otherwise(metered_ceiling)

rules = {
    "wrong_month":   bad(F.date_format("pickup_ts", "yyyy-MM") != F.col("file_month")),
    "bad_duration":  bad(~duration_min.between(1, 180)),
    "bad_distance":  bad(~F.col("trip_distance").between(0.1, 100)),
    "fare_too_low":  bad(F.col("fare_amount") < 3.0),
    "fare_too_high": bad(F.col("fare_amount") > max_fare),
    "missing_zone":  bad(F.col("pu_zone").isNull() | F.col("do_zone").isNull()),
}

flagged = raw
for name, cond in rules.items():
    flagged = flagged.withColumn(name, cond)

report = flagged.agg(F.count("*").alias("total"),
                     *[F.sum(F.col(r).cast("int")).alias(r) for r in rules]).first()
print("---- QUALITY REPORT (rows failing each rule) ----")
for k, v in report.asDict().items():
    print(f"{k:14s} {v:>12,}")

any_bad = reduce(lambda a, b: a | b, [F.col(r) for r in rules])
clean = (flagged.filter(~any_bad)
         .drop(*rules.keys())
         .dropDuplicates(["pickup_ts", "dropoff_ts", "pu_zone", "do_zone", "total_amount"]))

clean.write.mode("overwrite").partitionBy("file_month").parquet("data/silver/yellow")

silver_rows = spark.read.parquet("data/silver/yellow").count()
print(f"silver rows: {silver_rows:,}  kept: {100 * silver_rows / report['total']:.2f}%")
spark.stop()