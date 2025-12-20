#Extract registration date and registration time into separate columns.
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    date_format
)

# -----------------------------------
# 1. Create Spark Session
# -----------------------------------
spark = SparkSession.builder \
    .appName("Customer Registration Date Time Extraction CSV") \
    .master("local[*]") \
    .getOrCreate()

# -----------------------------------
# 2. Read CSV File
# -----------------------------------
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("customers.csv")   # use absolute path if needed

print("=== Original Data ===")
df.show(truncate=False)
df.printSchema()

# -----------------------------------
# 3. Convert String → Timestamp
# -----------------------------------
df_ts = df \
    .withColumn(
        "registration_ts",
        to_timestamp(col("registration_datetime"), "dd-MM-yyyy HH:mm:ss")
    ) \
    .withColumn(
        "last_login_ts",
        to_timestamp(col("last_login_datetime"), "dd-MM-yyyy HH:mm:ss")
    )

print("=== After Timestamp Conversion ===")
df_ts.show(truncate=False)
df_ts.printSchema()

# -----------------------------------
# 4. Extract Registration Date & Time
# -----------------------------------
df_reg_parts = df_ts \
    .withColumn("registration_date", to_date(col("registration_ts"))) \
    .withColumn("registration_time", date_format(col("registration_ts"), "HH:mm:ss"))

# -----------------------------------
# 5. Final Output
# -----------------------------------
print("=== Final Result ===")
df_reg_parts.select(
    "customer_id",
    "customer_name",
    "city",
    "registration_date",
    "registration_time",
    "status"
).show(truncate=False)

# -----------------------------------
# 6. Stop Spark
# -----------------------------------
spark.stop()




#Calculate the number of days between registration and last login.
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    datediff
)

# -----------------------------------
# 1. Create Spark Session
# -----------------------------------
spark = SparkSession.builder \
    .appName("Days Between Registration and Last Login") \
    .master("local[*]") \
    .getOrCreate()

# -----------------------------------
# 2. Read CSV File
# -----------------------------------
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("customers.csv")   # use absolute path if needed

print("=== Original Data ===")
df.show(truncate=False)
df.printSchema()

# -----------------------------------
# 3. Convert String → Timestamp
# -----------------------------------
df_ts = df \
    .withColumn(
        "registration_ts",
        to_timestamp(col("registration_datetime"), "dd-MM-yyyy HH:mm:ss")
    ) \
    .withColumn(
        "last_login_ts",
        to_timestamp(col("last_login_datetime"), "dd-MM-yyyy HH:mm:ss")
    )

print("=== After Timestamp Conversion ===")
df_ts.show(truncate=False)

# -----------------------------------
# 4. Calculate Days Between Registration & Last Login
# -----------------------------------
df_days_diff = df_ts.withColumn(
    "days_between_registration_and_last_login",
    datediff(col("last_login_ts"), col("registration_ts"))
)

# -----------------------------------
# 5. Final Output
# -----------------------------------
print("=== Days Between Registration and Last Login ===")
df_days_diff.select(
    "customer_id",
    "customer_name",
    "registration_ts",
    "last_login_ts",
    "days_between_registration_and_last_login",
    "status"
).show(truncate=False)

# -----------------------------------
# 6. Stop Spark
# -----------------------------------
spark.stop()

