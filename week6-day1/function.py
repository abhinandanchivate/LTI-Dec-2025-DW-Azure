# =========================================
# File Name : ReplaceNullLastLogin.py
# Purpose   : Replace NULL last_login_datetime
#             with "Not Logged In"
# =========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


# -----------------------------------------
# 1. INIT SECTION (Spark Initialization)
# -----------------------------------------
def init_spark():
    spark = SparkSession.builder \
        .appName("Replace Null Last Login - Java Style") \
        .master("local[*]") \
        .getOrCreate()

    return spark


# -----------------------------------------
# 2. PROCESS SECTION (Business Logic)
# -----------------------------------------
def process_replace_null_last_login(df):
    processed_df = df.withColumn(
        "last_login_datetime",
        when(
            col("last_login_datetime").isNull(),
            "Not Logged In"
        ).otherwise(
            col("last_login_datetime").cast("string")
        )
    )

    return processed_df


# -----------------------------------------
# 3. EXECUTION SECTION (Job Runner)
# -----------------------------------------
def main():
    # Step 1: Initialize Spark
    spark = init_spark()

    # Step 2: Input Data (simulating CSV input)
    data = [
        ("C001", "2024-12-10 18:30:00"),
        ("C002", None),
        ("C003", "2024-11-05 09:15:00")
    ]

    columns = ["customer_id", "last_login_datetime"]

    input_df = spark.createDataFrame(data, columns)

    # Step 3: Apply Processing Logic
    result_df = process_replace_null_last_login(input_df)

    # Step 4: Output
    result_df.show(truncate=False)

    # Step 5: Stop Spark
    spark.stop()


# -----------------------------------------
# 4. PROGRAM ENTRY POINT (like Java main)
# -----------------------------------------
if __name__ == "__main__":
    main()
