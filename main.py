import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

# Step 1: Boston Employees
df_boston = pd.read_sql("""
    SELECT e.firstName, e.lastName, e.jobTitle
    FROM employees e
    INNER JOIN offices o ON e.officeCode = o.officeCode
    WHERE o.city = 'Boston'
""", conn)

# Step 2: Ghost Locations
df_zero_emp = pd.read_sql("""
    SELECT o.officeCode, o.city, COUNT(e.employeeNumber) AS employeeCount
    FROM offices o
    LEFT JOIN employees e ON o.officeCode = e.officeCode
    GROUP BY o.officeCode
    HAVING employeeCount = 0
""", conn)

# Step 3: All Employees (Audit)
# Renamed to df_contacts because CodeGrade specifically requested this name
df_contacts = pd.read_sql("""
    SELECT e.firstName, e.lastName, o.city, o.state
    FROM employees e
    LEFT JOIN offices o ON e.officeCode = o.officeCode
    ORDER BY e.firstName, e.lastName
""", conn)

# Step 4: Customers with no orders
df_no_orders = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
    FROM customers c
    LEFT JOIN orders o ON c.customerNumber = o.customerNumber
    WHERE o.orderNumber IS NULL
    ORDER BY c.contactLastName ASC
""", conn)

# Step 5: Customer Payments
df_payment = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
    FROM customers c
    INNER JOIN payments p ON c.customerNumber = p.customerNumber
    ORDER BY p.amount DESC
""", conn)

# Step 6: Trustworthy Sales Reps (High Credit)
df_credit = pd.read_sql("""
    SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) AS no_of_customers
    FROM employees e
    INNER JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY e.employeeNumber, e.firstName, e.lastName
    HAVING AVG(c.creditLimit) > 90000
    ORDER BY no_of_customers DESC
""", conn)

# Step 7: Top Selling Products
df_product_sold = pd.read_sql("""
    SELECT p.productName, COUNT(od.productCode) AS numorders, SUM(od.quantityOrdered) AS totalunits
    FROM products p
    INNER JOIN orderdetails od ON p.productCode = od.productCode
    GROUP BY p.productCode, p.productName
    ORDER BY totalunits DESC
""", conn)

# Step 8: Market Reach (Unique Purchasers)
df_total_customers = pd.read_sql("""
    SELECT p.productName, p.productCode, COUNT(DISTINCT o.customerNumber) AS numpurchasers
    FROM products p
    INNER JOIN orderdetails od ON p.productCode = od.productCode
    INNER JOIN orders o ON od.orderNumber = o.orderNumber
    GROUP BY p.productCode, p.productName
    ORDER BY numpurchasers DESC
""", conn)

# Step 9: Customers per Office
df_customers = pd.read_sql("""
    SELECT o.officeCode, o.city, COUNT(c.customerNumber) AS n_customers
    FROM offices o
    INNER JOIN employees e ON o.officeCode = e.officeCode
    INNER JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY o.officeCode, o.city
    ORDER BY n_customers DESC
""", conn)

# Step 10: Underperforming Products Subquery
df_under_20 = pd.read_sql("""
    SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
    FROM employees e
    INNER JOIN offices o ON e.officeCode = o.officeCode
    INNER JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    INNER JOIN orders ord ON c.customerNumber = ord.customerNumber
    INNER JOIN orderdetails od ON ord.orderNumber = od.orderNumber
    WHERE od.productCode IN (
        SELECT productCode
        FROM orderdetails
        INNER JOIN orders ON orderdetails.orderNumber = orders.orderNumber
        GROUP BY productCode
        HAVING COUNT(DISTINCT customerNumber) < 20
    )
""", conn)

# Close the connection
conn.close()