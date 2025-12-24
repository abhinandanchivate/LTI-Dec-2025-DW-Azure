from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Sliding Window Example") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.options(multiline=True).json("transactions.json")

df = df.withColumn(
    "txn_time",
    to_timestamp("txn_time")
)

df.orderBy("txn_time").show()
df.printSchema()

sliding_window = Window.partitionBy("user") \
                       .orderBy("txn_time") \
                       .rowsBetween(-1, Window.currentRow)

df_sliding = df.withColumn(
    "avg_last_2_txns",
    avg("amount").over(sliding_window)
)

df_sliding.show()
time_window = Window.partitionBy("user") \
    .orderBy(col("txn_time").cast("long")) \
    .rangeBetween(-600, 0)   # 600 seconds = 10 minutes

df_time_sliding = df.withColumn(
    "sum_last_10_mins",
    sum("amount").over(time_window)
)

df_time_sliding.show()
