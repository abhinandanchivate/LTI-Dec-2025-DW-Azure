Level 1: Basic PySpark Questions

Read the customer CSV file into a PySpark DataFrame with headers.

Display only customer_id, customer_name, and city.

Count the total number of customers.

Show all customers whose status is ACTIVE.

Find customers who belong to the city Pune.

Rename column customer_name to full_name.

Sort customers by customer_name in ascending order.

Show distinct list of cities.

Check how many customers are INACTIVE.

Display schema of the DataFrame.


Level 2: Date & Time (Timestamp) Questions

Convert registration_datetime and last_login_datetime from string to TimestampType.

Extract registration date and registration time into separate columns.

Find customers who registered in the year 2024.

Identify customers who never logged in.

Calculate the number of days between registration and last login.

Find customers whose last login happened in December 2024.

Display customers who logged in after 6 PM.

Find the earliest and latest registration timestamps.

Sort customers by most recent login.

Replace null last_login_datetime with "Not Logged In".


Level 3: Filtering & Conditional Logic

Categorize customers as:

NEW → registered within last 30 days

OLD → otherwise

Mark customers as DORMANT if last login is older than 90 days.

Filter customers whose email domain is gmail.com.

Find customers with phone numbers starting with 98.

Show customers whose name starts with letter A.

Create a new column login_status:

LOGGED_IN if last_login is not null

NEVER_LOGGED_IN otherwise

Convert all city names to uppercase.

Remove customers with missing email addresses.

Filter customers registered between Jan 2024 and Mar 2024.

Mask phone numbers except last 4 digits.

Level 4: Aggregations & Grouping

Count customers per city.

Count ACTIVE vs INACTIVE customers.

Find city with maximum customers.

Find average login gap (days) per city.

Get count of customers who never logged in.

Group customers by registration month.

Find how many customers logged in per day.

Identify cities having more than one ACTIVE customer.

Find the percentage of ACTIVE customers.

Rank customers based on most recent login.

