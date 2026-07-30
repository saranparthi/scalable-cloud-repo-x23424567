from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test") \
    .master("local[*]") \
    .getOrCreate()

print("Spark version: " + spark.version)
print("Spark session created successfully")

df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "value"])
df.show()

spark.stop()
