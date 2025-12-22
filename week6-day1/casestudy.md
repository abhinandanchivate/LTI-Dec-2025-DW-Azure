

* **3 realistic CSV files**
* **30 scenario-based PySpark questions**
* Difficulty level: **Average → Above Average**
* Questions are **business-driven**, not toy problems
* Designed so **each CSV is actually required** (joins, time logic, windowing, data quality, etc.)

---

# 📁 Dataset 1: `customers.csv`

**Customer master data**

```csv
customer_id,customer_name,email,phone,city,registration_datetime,last_login_datetime,status
C001,Rahul Sharma,rahul@gmail.com,9876543210,Pune,2024-01-10 09:15:00,2024-12-20 21:10:00,ACTIVE
C002,Anita Verma,anita@gmail.com,9876500001,Mumbai,2024-02-05 10:30:00,,ACTIVE
C003,Suresh Iyer,suresh@gmail.com,9876500002,Chennai,2023-11-18 08:45:00,2024-06-15 17:20:00,INACTIVE
C004,Priya Nair,priya@gmail.com,9876500003,Bangalore,2024-03-22 14:00:00,2024-12-05 19:45:00,ACTIVE
C005,Amit Patel,amit@gmail.com,9876500004,Ahmedabad,2024-01-01 11:10:00,2024-01-01 11:10:00,ACTIVE
```

---

# 📁 Dataset 2: `orders.csv`

**Transactional order data**

```csv
order_id,customer_id,order_datetime,order_amount,payment_mode,order_status
O1001,C001,2024-12-01 18:30:00,2500,CARD,DELIVERED
O1002,C001,2024-12-15 21:15:00,1800,UPI,DELIVERED
O1003,C002,2024-11-20 10:00:00,3200,COD,CANCELLED
O1004,C003,2024-06-10 16:45:00,1500,CARD,DELIVERED
O1005,C004,2024-12-03 20:30:00,4500,CARD,DELIVERED
O1006,C004,2024-12-25 22:10:00,5200,UPI,DELIVERED
O1007,C005,2024-01-01 12:00:00,999,COD,DELIVERED
```

---

# 📁 Dataset 3: `support_tickets.csv`

**Customer support / complaints data**

```csv
ticket_id,customer_id,issue_type,created_datetime,resolved_datetime,priority,status
T001,C001,Payment Issue,2024-12-02 09:30:00,2024-12-02 12:45:00,HIGH,CLOSED
T002,C002,Login Issue,2024-12-05 10:00:00,,MEDIUM,OPEN
T003,C003,Refund Delay,2024-06-12 11:20:00,2024-06-20 16:00:00,HIGH,CLOSED
T004,C004,Order Not Received,2024-12-04 18:00:00,2024-12-06 14:30:00,CRITICAL,CLOSED
T005,C004,App Crash,2024-12-26 09:15:00,,HIGH,OPEN
```

---

# 🧠 30 Scenario-Based PySpark Questions

**(Average → Above Average Difficulty)**

---

## 🔹 Section A: Customer Analytics (Q1–Q10)

1️⃣ Identify customers who **registered in 2024 but never logged in**.

2️⃣ Find customers whose **last login happened after 8 PM**.

3️⃣ Calculate **days since last login** for each customer and flag customers as `DORMANT` if > 90 days.

4️⃣ Identify **cities with more than 1 ACTIVE customer**.

5️⃣ Find customers who **logged in on the same day as registration**.

6️⃣ Replace NULL `last_login_datetime` with **"Not Logged In"** (string-based output).

7️⃣ Rank customers **city-wise by registration date** (earliest first).

8️⃣ Identify customers whose **account age > 180 days** but **status is still ACTIVE**.

9️⃣ Extract **registration hour** and analyze peak registration hours.

🔟 Detect **data quality issues** where `last_login_datetime < registration_datetime`.

---

## 🔹 Section B: Orders & Revenue Analysis (Q11–Q20)

1️⃣1️⃣ Find **total revenue per customer** (DELIVERED orders only).

1️⃣2️⃣ Identify customers who placed **more than 1 order in December 2024**.

1️⃣3️⃣ Find **customers with orders but status = INACTIVE** (cross-dataset validation).

1️⃣4️⃣ Calculate **average order value per payment mode**.

1️⃣5️⃣ Identify orders placed **after 9 PM** and mark them as `LATE_NIGHT_ORDER`.

1️⃣6️⃣ Find customers whose **first order amount > ₹3000**.

1️⃣7️⃣ Compute **month-wise revenue trend** for 2024.

1️⃣8️⃣ Identify customers who **never placed any order**.

1️⃣9️⃣ Rank customers by **total spend** (highest to lowest).

2️⃣0️⃣ Detect **suspicious customers** where `COD orders > CARD orders`.

---

## 🔹 Section C: Support & Experience Analytics (Q21–Q30)

2️⃣1️⃣ Identify customers who raised **support tickets within 24 hours of an order**.

2️⃣2️⃣ Calculate **average ticket resolution time** (in hours) by priority.

2️⃣3️⃣ Identify customers with **OPEN tickets and ACTIVE status**.

2️⃣4️⃣ Find customers who placed orders but **never raised any support ticket**.

2️⃣5️⃣ Identify **repeat issue customers** (more than 1 ticket).

2️⃣6️⃣ Detect **high-risk customers**:

* ACTIVE
* HIGH / CRITICAL ticket
* OPEN status

2️⃣7️⃣ Calculate **order-to-ticket ratio per customer**.

2️⃣8️⃣ Identify customers whose **ticket resolution time > 3 days**.

2️⃣9️⃣ Create a **customer health score**:

* +10 → Order placed
* –5 → Ticket raised
* –10 → Ticket still OPEN

3️⃣0️⃣ Identify customers who:

* Logged in after 8 PM
* Placed orders after 9 PM
* Raised HIGH priority tickets

---

# 🎯 Why this set is strong for interviews & training

✔ Uses **all 3 CSVs meaningfully**
✔ Forces use of:

* Joins
* Date/time functions
* Conditional logic
* Window functions
* Data quality checks
  ✔ Mirrors **real production analytics scenarios**
  ✔ Perfect for:
* Assignments
* Labs
* Assessments
* Interview prep

---

