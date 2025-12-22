from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Customer PySpark SQL") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv(
    "customers.csv",
    header=True,
    inferSchema=True
)

df.createOrReplaceTempView("customers")
# it registers the DataFrame as a SQL temporary view / table
# table or view  customers we are able to run SQL queries against it.
spark.sql("""
    CREATE OR REPLACE TEMP VIEW customers_ts AS
    SELECT
        customer_id,
        customer_name,
        email,
        phone,
        city,
        registration_datetime,
        last_login_datetime,
        status,
        total_spent,
        loyalty_points,

        CAST(registration_datetime AS TIMESTAMP) AS registration_ts,

        CASE
            WHEN last_login_datetime IS NULL OR last_login_datetime = ''
            THEN NULL
            ELSE CAST(last_login_datetime AS TIMESTAMP)
        END AS last_login_ts

    FROM customers
""")

customers_df = spark.sql("SELECT * FROM customers_ts")
customers_df.show(truncate=False)
