
---

# ✅ Recommended Versions (IMPORTANT)

| Component             | Version               | Why                        |
| --------------------- | --------------------- | -------------------------- |
| **Python**            | **3.11.x**            | Fully supported by PySpark |
| **Java (JDK)**        | **Java 11**           | Spark 3.x compatible       |
| **PySpark**           | **3.5.x**             | Latest stable              |
| **Spark**             | **3.5.x (Pre-built)** | Matches PySpark            |
| **Hadoop (winutils)** | **3.3.x**             | For Windows HDFS support   |

> ❌ Python **3.13 is NOT supported** by PySpark yet
> ❌ Java 17/21 causes runtime issues

---

# 1️⃣ Install Python 3.11 (Windows)

### 🔹 Download

👉 [https://www.python.org/downloads/release/python-3119/](https://www.python.org/downloads/release/python-3119/)

### 🔹 Install (VERY IMPORTANT)

✔ Check **“Add Python to PATH”**
✔ Install for **All Users**

### 🔹 Verify

```bat
python --version
pip --version
```

Expected:

```
Python 3.11.x
```

---

# 2️⃣ Install Java JDK 11

### 🔹 Download (Eclipse Temurin – Best)

👉 [https://adoptium.net/temurin/releases/?version=11](https://adoptium.net/temurin/releases/?version=11)

Download:

* **JDK 11**
* **Windows x64**
* **MSI**

### 🔹 Install

Default path:

```
C:\Program Files\Eclipse Adoptium\jdk-11.x.x
```

### 🔹 Set JAVA_HOME

1. Open **System Environment Variables**
2. Add:

```
JAVA_HOME = C:\Program Files\Eclipse Adoptium\jdk-11.x.x
```

3. Update PATH:

```
%JAVA_HOME%\bin
```

### 🔹 Verify

```bat
java -version
```

Expected:

```
openjdk version "11"
```

---

# 3️⃣ Install Apache Spark (Pre-built)

### 🔹 Download Spark

👉 [https://spark.apache.org/downloads/](https://spark.apache.org/downloads/)

Select:

* Spark **3.5.x**
* Package type: **Pre-built for Apache Hadoop 3**
* Download `.zip`

### 🔹 Extract

```
C:\spark
```

Folder should contain:

```
C:\spark\bin
C:\spark\conf
C:\spark\jars
```

---

# 4️⃣ Install Hadoop winutils (MANDATORY on Windows)

### 🔹 Download winutils

👉 [https://github.com/cdarlint/winutils](https://github.com/cdarlint/winutils)

Download:

```
hadoop-3.3.x.zip
```

### 🔹 Extract

```
C:\hadoop
```

Ensure:

```
C:\hadoop\bin\winutils.exe
```

---

# 5️⃣ Set Environment Variables (CRITICAL)

### 🔹 System Variables

| Variable       | Value                                          |
| -------------- | ---------------------------------------------- |
| JAVA_HOME      | `C:\Program Files\Eclipse Adoptium\jdk-11.x.x` |
| SPARK_HOME     | `C:\spark`                                     |
| HADOOP_HOME    | `C:\hadoop`                                    |
| PYSPARK_PYTHON | `python`                                       |

### 🔹 PATH (Add all)

```
%JAVA_HOME%\bin
%SPARK_HOME%\bin
%HADOOP_HOME%\bin
```

### 🔹 Restart PC (Mandatory)

---

# 6️⃣ Install PySpark (Python Side)

```bat
pip install pyspark
```

Verify:

```bat
pip show pyspark
```

---

# 7️⃣ Test PySpark (Terminal)

### 🔹 Python Test Script

Create `test_pyspark.py`

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySparkTest") \
    .master("local[*]") \
    .getOrCreate()

data = [("Abhi", 30), ("John", 25)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()

spark.stop()
```

Run:

```bat
python test_pyspark.py
```

Expected Output:

```
+----+---+
|Name|Age|
+----+---+
|Abhi| 30|
|John| 25|
+----+---+
```

---

# 8️⃣ Test Spark Shell

```bat
spark-shell
```

or PySpark shell:

```bat
pyspark
```

---

# 9️⃣ Optional (VS Code Setup – Recommended)

### 🔹 Install Extensions

* Python
* Pylance

### 🔹 Select Interpreter

```
Python 3.11.x
```

### 🔹 Run PySpark Scripts directly

---

# 🚨 Common Errors & Fixes

### ❌ `Java gateway process exited`

✔ Java version mismatch → use **Java 11**

### ❌ `winutils.exe not found`

✔ Ensure:

```
C:\hadoop\bin\winutils.exe
```

### ❌ `Python 3.13`

✔ Downgrade to **3.11**

---

# 🧠 Best Practice (Training / Real Projects)

| Use Case           | Tool               |
| ------------------ | ------------------ |
| Exploration        | Jupyter Notebook   |
| Production scripts | VS Code            |
| Cluster learning   | WSL2 / Linux       |
| Big data labs      | Local Spark + HDFS |

---


