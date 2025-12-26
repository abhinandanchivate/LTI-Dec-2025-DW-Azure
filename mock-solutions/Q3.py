
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum, avg, desc, unix_timestamp, when

# ------------------------------------------------------------
# Create Spark Session
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("FoodDeliveryDelayAnalysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------------------
# 1. Function: load_inline_data
# ------------------------------------------------------------
def load_inline_data(spark: SparkSession) -> DataFrame:
    """
    Create a PySpark DataFrame from inline delivery records.
    """
    data = [
        ("FD001", "Zomato", "2024-04-10 12:00:00", "2024-04-10 12:45:00", "Traffic"),
        ("FD002", "Swiggy", "2024-04-10 13:00:00", "2024-04-10 13:30:00", "Weather"),
        ("FD003", "Zomato", "2024-04-10 14:00:00", "2024-04-10 15:00:00", "Traffic"),
        ("FD004", "UberEats", "2024-04-10 15:00:00", "2024-04-10 15:15:00", "Restaurant Delay")
    ]

    columns = [
        "order_id",
        "partner",
        "expected_delivery",
        "actual_delivery",
        "delay_reason"
    ]

    return spark.createDataFrame(data, columns)

# ------------------------------------------------------------
# 2. Function: compute_delay
# ------------------------------------------------------------
def compute_delay(df: DataFrame) -> DataFrame:
    """
    Compute delivery delay in hours and add column `delay`.
    Delays must be non-negative.
    """
    return df.withColumn(
        "delay",
        (
            unix_timestamp(col("actual_delivery")) -
            unix_timestamp(col("expected_delivery"))
        ) / 60
    ).withColumn(
        "delay",
        when(col("delay") < 0, 0).otherwise(col("delay"))
    )

# ------------------------------------------------------------
# 3. Function: get_delayed_orders
# ------------------------------------------------------------
def get_delayed_orders(df: DataFrame, threshold: float) -> DataFrame:
    """
    Filter orders where computed delay is greater than threshold.
    """
    delayed_df = compute_delay(df)
    return delayed_df.filter(col("delay") > threshold)

# ------------------------------------------------------------
# 4. Function: most_delayed_partner
# ------------------------------------------------------------
def most_delayed_partner(df: DataFrame) -> DataFrame:
    """
    Find the partner with the highest total delay.
    """
    delayed_df = compute_delay(df)

    return delayed_df.groupBy("partner") \
        .agg(sum("delay").alias("total_delay")) \
        .orderBy(desc("total_delay")) \
        .limit(1)

# ------------------------------------------------------------
# 5. Function: most_common_delay_reason
# ------------------------------------------------------------
def most_common_delay_reason(df: DataFrame) -> DataFrame:
    """
    Find the delay reason with the highest average delay.
    """
    delayed_df = compute_delay(df)

    return delayed_df.groupBy("delay_reason") \
        .agg(avg("delay").alias("avg_delay")) \
        .orderBy(desc("avg_delay")) \
        .limit(1)

# ------------------------------------------------------------
# Main Execution (Testing / Output)
# ------------------------------------------------------------
if __name__ == "__main__":

    df = load_inline_data(spark)

    print("\n=== Original Delivery Data ===")
    df.show(truncate=False)

    print("\n=== Data With Computed Delay ===")
    compute_delay(df).show(truncate=False)

    print("\n=== Delayed Orders (delay > 40 mins) ===")
    get_delayed_orders(df, 40.0).show(truncate=False)

    print("\n=== Most Delayed Partner ===")
    most_delayed_partner(df).show(truncate=False)

    print("\n=== Most Common Delay Reason ===")
    most_common_delay_reason(df).show(truncate=False)

    spark.stop()
