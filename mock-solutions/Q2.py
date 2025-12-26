from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, FloatType
from pyspark.sql.functions import col, avg, desc, year
from typing import List
import datetime

def create_vehicle_schema_and_load(spark: SparkSession) -> DataFrame:
    """
    Define schema and load inline vehicle data.
    Schema:
      maker: String
      model: String
      manufacture_date: date
      units_sold: integer
      mileage: float
      status: String
    """
    schema = StructType([
        StructField("maker", StringType(), True),
        StructField("model", StringType(), True),
        StructField("manufacture_date", DateType(), True),
        StructField("units_sold", IntegerType(), True),
        StructField("mileage", FloatType(), True),
        StructField("status", StringType(), True)
    ])

    data = [
        ("Toyota", "Camry", datetime.date(2018, 5, 20), 15000, 28.0, "active"),
        ("Honda", "Civic", datetime.date(2019, 3, 15), 12000, 32.0, "active"),
        ("Ford", "Focus", datetime.date(2017, 7, 10), 8000, 25.0, "inactive"),
        ("Chevrolet", "Malibu", datetime.date(2020, 1, 5), 10000, 30.0, "active"),
        ("Nissan", "Altima", datetime.date(2016, 11, 30), 6000, 27.0, "inactive")
    ]

    return spark.createDataFrame(data, schema)

def filter_active_vehicles(df: DataFrame) -> DataFrame:
    """
    Filter the DataFrame to include only active vehicles.
    """
    return df.filter(col("status") == "active")

def top_n_brands_by_avg_mileage(df: DataFrame, n: int) -> DataFrame:
    """
    Compute the top N vehicle brands by average mileage.
    Returns dataframe
    """
    return (df.groupBy("maker")).agg(avg("mileaga").alias("average_mileage")) \
        .orderBy(desc("average_mileage")) \
        .limit(n).select("maker", "average_mileage")  

def calculate_vehicle_age(df: DataFrame, reference_year: int) -> DataFrame:
    """
    Add a new column 'vehicle_age' representing the age of the vehicle in years
    based on the reference year.
    """
    return df.withColumn(
        "vehicle_age",
        reference_year - year(col("manufacture_date"))
    )   

def top_highest_selling_models(df: DataFrame, n: int) -> DataFrame:
    """
    Identify the top N highest selling vehicle models.
    Returns dataframe
    """
    return df.orderBy(desc("units_sold")).limit(n).select("model", "units_sold")
