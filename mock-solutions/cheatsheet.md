
---

# 🚀 PySpark COMPLETE CHEAT SHEET (ONE-GO MASTER VERSION)

Covers:
✅ DataFrames
✅ createDataFrame
✅ Filters
✅ Date & Time (incl. `unix_timestamp`)
✅ Joins (ALL types)
✅ RDD (`map`, `flatMap`)
✅ JSON & Arrays
✅ Window Functions
✅ Performance & Optimization
✅ Read / Write
✅ Interview-critical APIs

---

## 1️⃣ Spark Session (Entry Point)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySparkMasterCheatSheet") \
    .master("local[*]") \
    .getOrCreate()
```

---

## 2️⃣ createDataFrame (ALL PATTERNS)

### A. From List of Tuples

```python
data = [
    (1, "Rahul", "Pune", 50000),
    (2, "Anita", "Mumbai", 60000)
]
cols = ["id", "name", "city", "salary"]

df = spark.createDataFrame(data, cols)
```

---

### B. From List of Dicts

```python
data = [
    {"id":1, "name":"Rahul", "salary":50000},
    {"id":2, "name":"Anita", "salary":60000}
]
df = spark.createDataFrame(data)
```

---

### C. With Explicit Schema

```python
from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("salary", IntegerType())
])

df = spark.createDataFrame(data, schema)
```

---

## 3️⃣ DataFrame Inspection

```python
df.show()
df.show(truncate=False)
df.printSchema()
df.describe().show()
df.count()
df.columns
```

---

## 4️⃣ Column Operations

```python
from pyspark.sql.functions import col, lit
```

```python
df.select("name", "salary")
df.withColumn("bonus", col("salary") * 0.2)
df.withColumnRenamed("city", "location")
df.drop("temp_col")
```

---

## 5️⃣ Filter / Where

```python
df.filter(col("salary") > 50000)
df.where(col("city") == "Pune")
```

### Multiple Conditions

```python
df.filter(
    (col("salary") > 40000) &
    (col("city").isin("Pune","Mumbai"))
)
```

### NULL Handling

```python
df.filter(col("last_login").isNull())
df.filter(col("last_login").isNotNull())
```

---

## 6️⃣ Conditional Logic (when / otherwise)

```python
from pyspark.sql.functions import when

df.withColumn(
    "grade",
    when(col("salary") >= 70000, "A")
    .when(col("salary") >= 50000, "B")
    .otherwise("C")
)
```

---

# 🕒 7️⃣ DATE & TIME FUNCTIONS (COMPLETE 🔥)

```python
from pyspark.sql.functions import *
```

### String → Date / Timestamp

```python
to_date("order_date", "dd-MM-yyyy")
to_timestamp("login_ts", "yyyy-MM-dd HH:mm:ss")
```

---

### UNIX TIMESTAMP (IMPORTANT)

```python
unix_timestamp(col("login_ts"), "yyyy-MM-dd HH:mm:ss")
from_unixtime(col("unix_ts"))
from_unixtime(col("unix_ts"), "yyyy-MM-dd")
```

---

### Current Date / Time

```python
current_date()
current_timestamp()
```

---

### Extract Date Parts

```python
year(col("date"))
month(col("date"))
dayofmonth(col("date"))
dayofweek(col("date"))
weekofyear(col("date"))
hour(col("ts"))
minute(col("ts"))
second(col("ts"))
```

---

### Date Arithmetic

```python
datediff(col("end"), col("start"))
months_between(col("end"), col("start"))
date_add(col("date"), 10)
date_sub(col("date"), 5)
```

---

### Month / Year Helpers

```python
last_day(col("date"))
add_months(col("date"), 2)
trunc(col("date"), "MM")
trunc(col("date"), "YYYY")
next_day(col("date"), "Sunday")
```

---

### Date Formatting

```python
date_format(col("date"), "yyyy-MM")
date_format(col("date"), "dd/MM/yyyy")
```

---

## 8️⃣ String Functions

```python
upper(col("city"))
lower(col("name"))
length(col("name"))
substring(col("phone"), -4, 4)
concat(col("first"), lit(" "), col("last"))
```

---

## 9️⃣ Arrays / Structs / explode (REAL-WORLD ❗)

```python
from pyspark.sql.functions import explode, posexplode, array, struct

df.withColumn("item", explode(col("items")))
df.select(posexplode(col("items")))

array(col("a"), col("b"))
struct(col("name"), col("salary"))
size(col("items"))
array_contains(col("items"), "apple")
```

---

## 🔗 🔟 JOINS (ALL TYPES)

```python
df1.join(df2, "id", "inner")
df1.join(df2, "id", "left")
df1.join(df2, "id", "right")
df1.join(df2, "id", "full")
```

### Semi / Anti Joins

```python
df1.join(df2, "id", "left_semi")
df1.join(df2, "id", "left_anti")
```

### Join with Condition + Alias

```python
df1.alias("a").join(
    df2.alias("b"),
    col("a.id") == col("b.emp_id"),
    "inner"
).select("a.id","a.name","b.salary")
```

### Broadcast Join (Performance)

```python
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "id")
```

---

## 🔁 1️⃣1️⃣ RDD OPERATIONS

```python
rdd = spark.sparkContext.parallelize(["hello spark", "pyspark rocks"])
```

### map

```python
rdd.map(lambda x: x.upper()).collect()
```

### flatMap

```python
rdd.flatMap(lambda x: x.split(" ")).collect()
```

### filter

```python
rdd.filter(lambda x: "spark" in x).collect()
```

### DataFrame ↔ RDD

```python
df.rdd
rdd.toDF()
```

---

## 1️⃣2️⃣ Aggregations & GroupBy

```python
df.groupBy("city").count()
df.groupBy("dept").agg(
    avg("salary").alias("avg_sal"),
    max("salary").alias("max_sal")
)
```

---

## 🪟 1️⃣3️⃣ Window Functions

```python
from pyspark.sql.window import Window

w = Window.partitionBy("dept").orderBy(col("salary").desc())
df.withColumn("rank", rank().over(w))
```

### Running Total

```python
w = Window.partitionBy("dept") \
    .orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("running_sum", sum("salary").over(w))
```

---

## 1️⃣4️⃣ JSON Functions

```python
get_json_object(col("json"), "$.name")
from_json(col("json"), schema)
to_json(struct("*"))
```

---

## 1️⃣5️⃣ Deduplication & Sampling

```python
df.dropDuplicates()
df.dropDuplicates(["id","date"])
df.sample(fraction=0.1, seed=42)
```

---

## 1️⃣6️⃣ Repartition / Coalesce

```python
df.repartition(4)   # shuffle
df.coalesce(2)      # no shuffle
```

---

## 1️⃣7️⃣ Cache / Persist

```python
df.cache()

from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)
```

---

## 1️⃣8️⃣ explain() (INTERVIEW FAVORITE)

```python
df.explain()
df.explain(True)
```

---

## 1️⃣9️⃣ Writing Data (CRITICAL)

```python
df.write.mode("overwrite").csv("out/")
df.write.mode("append").parquet("out/")
df.write.partitionBy("city").parquet("out/")
```

---

## 2️⃣0️⃣ Handling Corrupt Records

```python
spark.read \
    .option("mode","PERMISSIVE") \
    .option("columnNameOfCorruptRecord","_corrupt_record") \
    .csv("data.csv")
```

---

## 2️⃣1️⃣ SQL Temp Views

```python
df.createOrReplaceTempView("employees")
spark.sql("SELECT city, AVG(salary) FROM employees GROUP BY city").show()
```

---

## 2️⃣2️⃣ Actions (Trigger Execution)

```python
df.show()
df.count()
df.take(5)
df.collect()
```

---

# 🎯 FINAL SUMMARY (FOR INTERVIEWS / TRAINING)

| Area        | Must Know                               |
| ----------- | --------------------------------------- |
| Date        | to_date, unix_timestamp, months_between |
| Logic       | when / otherwise                        |
| Joins       | anti, semi, broadcast                   |
| Arrays      | explode                                 |
| Performance | cache, explain, repartition             |
| IO          | write + partition                       |

---

