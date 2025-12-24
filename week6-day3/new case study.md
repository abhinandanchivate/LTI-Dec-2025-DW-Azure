

---

# 🏦 Finance – Loan Repayment Risk Analysis (PySpark Case Study)

**Question Code:** Q472

---

## 📌 Context

A **banking institution** wants to analyze **loan repayment transactions** to identify **high-risk loan accounts**.
Late payments, unusually high EMI amounts, and branch mismatches are considered potential risk indicators.

Your task is to build a **PySpark-based data processing pipeline** to:

* Load loan repayment data
* Flag high EMI payments
* Detect branch mismatches
* Generate loan-level risk summaries

⚠️ **Important Note**
The **first function (data loading)** is mandatory.
If the DataFrame is not created correctly, **no test cases will pass**.

---

## 🎯 Objective

You must implement **four pure PySpark functions** (no class usage):

1. Load loan repayment data
2. Flag high EMI payments
3. Detect branch mismatch
4. Generate loan risk summary

✔ All functions **must return a PySpark DataFrame**
✔ No side effects, no printing inside functions

---

## 🛠 How to Run the Assignment

### ✅ Create Python Virtual Environment

```bash
cd Question_folder_name
python3 -m venv venv
source venv/bin/activate
```

### ✅ Install Required Libraries

```bash
pip install --upgrade pip
pip install pyspark pytest
```

### ✅ Run Test Suite

```bash
python3 drivers/run_json.py
```

### ✅ Output

* Console shows `[PASS] / [FAIL]`
* Detailed report written to:

```
student_submission/test_report.log
```

---

## 📄 Input Dataset (CSV)

Example columns in `data/input.csv`:

| Column Name    | Description                  |
| -------------- | ---------------------------- |
| loan_id        | Unique loan account number   |
| customer_id    | Customer identifier          |
| emi_amount     | Monthly EMI amount           |
| home_branch    | Branch where loan was issued |
| payment_branch | Branch where EMI was paid    |
| payment_date   | EMI payment date             |
| payment_status | PAID / LATE                  |

---

## ✅ Function Specifications

---

## ✅ 1️⃣ Load Loan Repayment Data

### Function Name

`load_loan_data`

### Parameters

* `spark: SparkSession`
* `path: str`

### Returns

* `DataFrame`

### Description

Loads a CSV file using:

* Header recognition
* Schema inference

### Function Signature

```python
def load_loan_data(spark: SparkSession, path: str) -> DataFrame:
```

### Example Input

```python
load_loan_data(spark, "data/input.csv")
```

### Expected Output

A DataFrame with inferred columns:

* loan_id
* customer_id
* emi_amount
* home_branch
* payment_branch
* payment_date
* payment_status

---

## ✅ 2️⃣ Flag High EMI Payments

### Function Name

`flag_high_emi`

### Parameters

* `df: DataFrame`
* `threshold: float = 50000.0`

### Returns

* `DataFrame`

### Description

Adds a new boolean column:

| Column Name   | Condition                                        |
| ------------- | ------------------------------------------------ |
| `is_high_emi` | `True` if `emi_amount > threshold`, else `False` |

### Function Signature

```python
def flag_high_emi(df: DataFrame, threshold: float = 50000.0) -> DataFrame:
```

---

## ✅ 3️⃣ Detect Branch Mismatch

### Function Name

`detect_branch_mismatch`

### Parameters

* `df: DataFrame`

### Returns

* `DataFrame`

### Description

Adds a new boolean column:

| Column Name       | Condition                                               |
| ----------------- | ------------------------------------------------------- |
| `branch_mismatch` | `True` if `home_branch != payment_branch`, else `False` |

### Function Signature

```python
def detect_branch_mismatch(df: DataFrame) -> DataFrame:
```

---

## ✅ 4️⃣ Summarize Loan Risk Indicators

### Function Name

`summarize_loan_risk`

### Parameters

* `df: DataFrame`

### Returns

* `DataFrame`

### Description

Generate a **loan-level summary** with:

| Column         | Description              |
| -------------- | ------------------------ |
| loan_id        | Loan account             |
| total_payments | Count of payment records |

✔ Use `groupBy` and `count`
✔ No sorting required

### Function Signature

```python
def summarize_loan_risk(df: DataFrame) -> DataFrame:
```

---

## 🔄 Implementation Flow (Expected by Tests)

1. Load CSV into DataFrame
2. Add `is_high_emi` column
3. Add `branch_mismatch` column
4. Aggregate by `loan_id`
5. Return final DataFrame

---

## 📊 Real-World Relevance

This case study simulates:

* **Retail banking risk analytics**
* **Loan monitoring systems**
* **Early warning signals for defaults**
* **Branch compliance validation**

It closely mirrors how **banks build pre-risk dashboards** using Spark pipelines before feeding data into ML models.

---

## 🧠 Difficulty Level

✔ Intermediate
✔ Suitable for:

* PySpark beginners moving to real use cases
* Data engineering interviews
* Assignment-based evaluations
* BFSI training programs

---

