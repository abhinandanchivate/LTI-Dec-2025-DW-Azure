# -----------------------------------------
# 1. Imports
# -----------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, BooleanType


# -----------------------------------------
# 2. Spark Session Initialization
# -----------------------------------------
def create_spark_session():
    return SparkSession.builder \
        .appName("Customer UDF Example") \
        .master("local[*]") \
        .getOrCreate()


# -----------------------------------------
# 3. Read Input JSON
# -----------------------------------------
def read_customer_data(spark):
    df = spark.read.json("customers.json")
    print("=== Input Data ===")
    df.show()
    return df


# -----------------------------------------
# 4. Business Logic - UDFs
# -----------------------------------------

# UDF 1: Categorize customer spend
def spend_category(amount):
    if amount < 1000:
        return "LOW"
    elif amount <= 3000:
        return "MEDIUM"
    else:
        return "HIGH"


# UDF 2: Identify VIP customer
def is_vip(city, spend):
    return city == "Pune" and spend > 2000


# -----------------------------------------
# 5. Apply UDF Transformations
# -----------------------------------------
def apply_udfs(df):

    # Register UDFs
    spend_udf = udf(spend_category, StringType())
    vip_udf = udf(is_vip, BooleanType())

    # Apply spend category UDF
    df_with_category = df.withColumn(
        "spend_category",
        spend_udf(col("spend"))
    )

    print("=== After Spend Category UDF ===")
    df_with_category.show()

    # Apply VIP UDF
    df_with_vip = df_with_category.withColumn(
        "is_vip",
        vip_udf(col("city"), col("spend"))
    )

    print("=== After VIP UDF ===")
    df_with_vip.show()

    return df_with_vip


# -----------------------------------------
# 6. Main Application Flow
# -----------------------------------------
def main():
    spark = create_spark_session()
    df = read_customer_data(spark)
    final_df = apply_udfs(df)
    spark.stop()


# -----------------------------------------
# 7. Entry Point
# -----------------------------------------
if __name__ == "__main__":
    main()
