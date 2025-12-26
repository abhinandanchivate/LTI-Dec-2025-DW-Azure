from typing import Tuple, Optional

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, to_date, datediff, count, desc, asc , when, current_date


def load_circulation_data(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "true").csv(path, inferSchema=True).withColumn("borrow_date", to_date("borrow_date", "yyyy-MM-dd")).withColumn("return_date", to_date("return_date", "yyyy-MM-dd"))

def with_late_days(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "days",
        when(
            col("return_date").isNotNull(),
            datediff(col("return_date"), col("due_date"))
        ).otherwise(
            datediff(current_date(), col("borrow_date"))
        )
    ).withColumn(
        "days",
        when(col("days") > 0, col("days")).otherwise(0)
    )

def filter_valid_loans(df: DataFrame) -> DataFrame:
    return df.filter(col("days")>=0)

def category_popularity(df: DataFrame) -> DataFrame:
   return df.groupBy("category").agg(count("*").alias("borrows"))

def top_category(df: DataFrame) -> Tuple[str, int]:
    #The variable can be of type T OR None
    row: Optional[Row] = (
        df.orderBy(desc("borrows"), asc("category"))
          .select("category", "borrows")
          .first()
    )

    if row is None:
        return ("", 0)

    return (row["category"], int(row["borrows"]))

