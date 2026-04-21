import sqlite3
import pandas as pd
import numpy as np

conn = sqlite3.connect('data.sqlite')

# Part 1: Step 1
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

# Part 2: Step 3
df_employee = pd.read_sql("""
    SELECT e.firstName, e.lastName, o.city, o.state
    FROM employees e
    LEFT JOIN offices o ON e.officeCode = o.officeCode
    ORDER BY e.firstName, e.lastName
""", conn)

# Part 2: Step 4 — RENAMED to df_contacts
df_contacts = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
    FROM customers c
    LEFT JOIN orders o ON c.customerNumber = o.customerNumber
    WHERE o.orderNumber IS NULL
    ORDER BY c.contactLastName ASC
""", conn)

# Updated Step 5
df_payment = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
    FROM customers c
    JOIN payments p ON c.customerNumber = p.customerNumber
    ORDER BY CAST(p.amount AS REAL) DESC
""", conn)
# Part 4: Step 6 & 7
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

# Part 5: Step 8
df_total_customers = pd.read_sql("""
    SELECT p.productName, p.productCode, COUNT(DISTINCT o.customerNumber) AS numpurchasers
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    JOIN orders o ON od.orderNumber = o.orderNumber
    GROUP BY 1, 2
    ORDER BY numpurchasers DESC
""", conn)
#step 9
df_customers = pd.read_sql("""
    SELECT o.officeCode, o.city, COUNT(DISTINCT c.customerNumber) AS n_customers
    FROM offices o
    JOIN employees e ON o.officeCode = e.officeCode
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY o.officeCode, o.city
    ORDER BY n_customers DESC
""", conn)

## Part 6: Step 10 — find employees who have customers that ONLY ordered 
# products purchased by fewer than 20 unique customers total
# Fixed Step 10
df_under_20 = pd.read_sql("""
    SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
    FROM employees e
    JOIN offices o ON e.officeCode = o.officeCode
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    WHERE c.customerNumber NOT IN (
        SELECT DISTINCT customerNumber
        FROM orders
        JOIN orderdetails USING (orderNumber)
        WHERE productCode IN (
            SELECT productCode
            FROM orderdetails
            JOIN orders USING (orderNumber)
            GROUP BY productCode
            HAVING COUNT(DISTINCT customerNumber) >= 20
        )
    )
    ORDER BY e.firstName ASC
""", conn)
conn.close()