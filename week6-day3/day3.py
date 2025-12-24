from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Window Functions JSON Example") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.json("orders.json")

df = df.withColumn(
    "order_date",
    to_date("order_date", "yyyy-MM-dd")
)

df.show()
df.printSchema()
