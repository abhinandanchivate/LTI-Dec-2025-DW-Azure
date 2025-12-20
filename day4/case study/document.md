
---

# 📊 Enterprise Case Study Documentation

## Customer Activity & Engagement Analytics using PySpark (CSV-Based)

---

## 1️⃣ Business Overview

A large **multi-city retail e-commerce organization** operates web and mobile platforms.
Customer data is generated daily from multiple channels and stored as **CSV files**.

The organization wants to build a **scalable analytics solution** using **Apache Spark (PySpark)** to:

* Understand customer onboarding trends
* Track login behavior and engagement
* Identify inactive and churn-prone customers
* Optimize marketing and CRM strategies

---

## 2️⃣ Data Source Description

### 📁 Source Type

CSV Files (Batch ingestion)

### 📄 File Name

`customers.csv`

### 📌 Data Origin

* Web applications
* Mobile apps
* CRM systems

### 📊 Schema Overview

| Column                | Description                                  |
| --------------------- | -------------------------------------------- |
| customer_id           | Unique customer identifier                   |
| customer_name         | Customer full name                           |
| email                 | Email address                                |
| phone                 | Contact number                               |
| city                  | Customer city                                |
| registration_datetime | Registration date & time (string)            |
| last_login_datetime   | Last login date & time (string / null)       |
| status                | Account status (Active / Inactive / Blocked) |

---

## 3️⃣ Key Data Challenges (Real-World)

* Date and time stored as **string values**
* Missing or null login timestamps
* Large volume of records (millions)
* Time-based analysis required
* Business users need **human-readable outputs**

---

## 4️⃣ Analytics Objectives

The analytics team must answer **business-driven questions** using PySpark functions on CSV data.

---

## 5️⃣ Business Requirements & Analytics Criteria

---

### ✅ Criteria 1: Convert String Date Columns to Timestamp

**Why:**
Time-based calculations are not possible on string data.

**Analytics Focus:**
Enable hour, day, month, year-level analytics.

**Business Value:**
Foundation for all downstream analysis.

---

### ✅ Criteria 2: Extract Registration Date & Time Separately

**Why:**
Marketing teams analyze onboarding patterns by **date** and **time slot**.

**Business Value:**

* Identify peak registration hours
* Optimize onboarding campaigns

---

### ✅ Criteria 3: Identify Customers Registered in a Specific Year (e.g., 2024)

**Why:**
Track new customer acquisition year-wise.

**Business Value:**

* Yearly growth analysis
* Budget planning

---

### ✅ Criteria 4: Identify Customers Who Never Logged In

**Why:**
Some users register but never engage.

**Business Value:**

* Re-engagement campaigns
* Account cleanup
* User journey optimization

---

### ✅ Criteria 5: Calculate Days Between Registration and Last Login

**Why:**
Measure how quickly customers become active.

**Business Value:**

* Engagement health analysis
* User behavior insights

---

### ✅ Criteria 6: Identify Customers Logged in During a Specific Month (December 2024)

**Why:**
Festive season traffic analysis.

**Business Value:**

* Campaign effectiveness
* Seasonal demand insights

---

### ✅ Criteria 7: Identify Customers Logging in After 6 PM

**Why:**
Evening usage patterns impact marketing and notifications.

**Business Value:**

* Optimal push notification timing
* Flash sale scheduling

---

### ✅ Criteria 8: Find Earliest & Latest Registrations

**Why:**
Understand platform adoption timeline.

**Business Value:**

* Growth trend tracking
* Platform maturity analysis

---

### ✅ Criteria 9: Sort Customers by Most Recent Login

**Why:**
Support and sales teams prioritize active users.

**Business Value:**

* VIP customer handling
* Faster issue resolution

---

### ✅ Criteria 10: Replace Null Login Values with Meaningful Labels

**Why:**
Business reports must be readable by non-technical users.

**Business Value:**

* Clear dashboards
* Improved reporting quality

---

## 6️⃣ Advanced / Complex Analytics Criteria

---

### ✅ Criteria 11: Detect Irregular Login Patterns (Security Analysis)

**Problem:**
Unusual frequent logins may indicate bots or account misuse.

**Analytics Approach:**
Analyze time gaps between consecutive logins per customer.

**Business Value:**

* Fraud detection
* Security alerts

---

### ✅ Criteria 12: Detect Long Inactivity Followed by Sudden Login

**Problem:**
Returning users after long inactivity are high-value prospects.

**Analytics Approach:**
Measure inactivity duration and identify sudden reactivation.

**Business Value:**

* Loyalty campaigns
* Personalized offers

---

### ✅ Criteria 13: City-Wise Peak Login Hour Analysis

**Problem:**
Different cities show different usage patterns.

**Analytics Approach:**
Aggregate login counts by city and hour.

**Business Value:**

* Geo-targeted marketing
* Regional optimization

---

### ✅ Criteria 14: Customer Engagement Score Calculation

**Problem:**
Business wants a single metric to rank customers.

**Scoring Factors:**

* Registration recency
* Login frequency
* Activity gaps
* Account status

**Business Value:**

* VIP segmentation
* Upsell targeting

---

### ✅ Criteria 15: Identify Potential Churn Customers

**Problem:**
Inactive users are likely to leave the platform.

**Analytics Approach:**
Calculate days since last login and apply churn thresholds.

**Business Value:**

* Proactive retention
* Reduced customer loss

---

## 7️⃣ End-to-End Analytics Flow

```
CSV Files
   ↓
Spark DataFrame Creation
   ↓
Schema Validation & Cleaning
   ↓
Date & Time Transformation
   ↓
Filtering & Window Analytics
   ↓
Behavioral Insights
   ↓
Business Reports / Dashboards
```

---

## 8️⃣ Departments Benefiting from This Analytics

| Department | Usage                            |
| ---------- | -------------------------------- |
| Marketing  | Campaign timing & segmentation   |
| CRM        | Re-engagement & churn prevention |
| Sales      | Active user prioritization       |
| Security   | Fraud & misuse detection         |
| Management | Growth & engagement insights     |

---

## 9️⃣ Why PySpark Is Used

* Handles **large CSV datasets efficiently**
* Distributed & fault-tolerant
* Supports SQL and DataFrame APIs
* Easy integration with cloud storage
* Ideal for batch + near real-time analytics

---

## 🔟 Learning Outcomes (For Trainees)

After this case study, learners can:

✔ Translate business questions into Spark logic
✔ Apply date & time functions correctly
✔ Perform behavioral analytics
✔ Handle nulls and real-world data issues
✔ Design production-ready analytics workflows

---


