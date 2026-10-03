from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
print("count:", df.count())      # test 1: does Spark run?
df.show()

df.write.mode("overwrite").parquet("test_out")   # test 2: can it write files?
print("rows read back:", spark.read.parquet("test_out").count())
spark.stop()