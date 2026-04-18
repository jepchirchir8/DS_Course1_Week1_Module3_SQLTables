import sqlite3
import pandas as pd
import numpy as np

conn = sqlite3.connect('data.sqlite')

# Part 1: Step 1 (ONLY 2 columns: firstName, lastName)
df_boston = pd.read_sql("""
    SELECT e.firstName, e.lastName
    FROM employees e
    JOIN offices o ON e.officeCode = o.officeCode
    WHERE o.city = 'Boston'
""", conn)

# Part 1: Step 2
df_zero_emp = pd.read_sql("""
    SELECT o.officeCode, o.city, COUNT(e.employeeNumber) AS employeeCount
    FROM offices o
    LEFT JOIN employees e ON o.officeCode = e.officeCode
    GROUP BY o.officeCode
    HAVING employeeCount = 0
""", conn)

# Part 2: Step 3 (Variable name MUST be df_employee)
df_employee = pd.read_sql("""
    SELECT e.firstName, e.lastName, o.city, o.state
    FROM employees e
    LEFT JOIN offices o ON e.officeCode = o.officeCode
    ORDER BY e.firstName, e.lastName
""", conn)

# Part 2: Step 4
df_no_orders = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
    FROM customers c
    LEFT JOIN orders o ON c.customerNumber = o.customerNumber
    WHERE o.orderNumber IS NULL
    ORDER BY c.contactLastName ASC
""", conn)

# Part 3: Step 5 (Sorting fix for Diego)
df_payment = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
    FROM customers c
    JOIN payments p ON c.customerNumber = p.customerNumber
    ORDER BY p.amount DESC
""", conn)

# Part 4: Step 6 & 7 (These passed, but keep names consistent)
df_credit = pd.read_sql("""
    SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) AS no_of_customers
    FROM employees e
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY 1, 2, 3
    HAVING AVG(c.creditLimit) > 90000
    ORDER BY no_of_customers DESC
""", conn)

df_product_sold = pd.read_sql("""
    SELECT p.productName, COUNT(od.productCode) AS numorders, SUM(od.quantityOrdered) AS totalunits
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    GROUP BY 1
    ORDER BY totalunits DESC
""", conn)

# Part 5: Step 8 & 9 (Fixing the n_customers count)
df_total_customers = pd.read_sql("""
    SELECT p.productName, p.productCode, COUNT(DISTINCT o.customerNumber) AS numpurchasers
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    JOIN orders o ON od.orderNumber = o.orderNumber
    GROUP BY 1, 2
    ORDER BY numpurchasers DESC
""", conn)

df_customers = pd.read_sql("""
    SELECT o.officeCode, o.city, COUNT(c.customerNumber) AS n_customers
    FROM offices o
    JOIN employees e ON o.officeCode = e.officeCode
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY 1, 2
    ORDER BY n_customers DESC
""", conn)

# Part 6: Step 10 (Fixing for 'Loui')
df_under_20 = pd.read_sql("""
    SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
    FROM employees e
    JOIN offices o ON e.officeCode = o.officeCode
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    JOIN orders ord ON c.customerNumber = ord.customerNumber
    JOIN orderdetails od ON ord.orderNumber = od.orderNumber
    WHERE od.productCode IN (
        SELECT productCode
        FROM orderdetails
        JOIN orders ON orderdetails.orderNumber = orders.orderNumber
        GROUP BY productCode
        HAVING COUNT(DISTINCT customerNumber) < 20
    )
    ORDER BY e.employeeNumber
""", conn)

conn.close()