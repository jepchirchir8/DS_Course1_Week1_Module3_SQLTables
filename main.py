# CodeGrade step0
# Run this cell without changes

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

df_boston1 = pd.read_sql("""
SELECT * FROM employees
""",conn)
df_boston1
df_boston2 = pd.read_sql("""
SELECT * FROM offices
""",conn)
df_boston2

df_boston = pd.read_sql("""
SELECT e.firstName, e.lastName
FROM employees e
INNER JOIN offices o ON e.officeCode = o.officeCode
WHERE o.city = 'Boston'
""", conn)

# CodeGrade step2
df_zero_emp = pd.read_sql("""
SELECT o.officeCode, o.city, COUNT(e.employeeNumber) AS employeeCount
FROM offices o
LEFT JOIN employees e ON o.officeCode = e.officeCode
GROUP BY o.officeCode
HAVING employeeCount = 0
""", conn)
df_zero_emp

# CodeGrade step3
# Replace None with your code
df_employee = pd.read_sql("""
SELECT e.firstName, e.lastName, o.city,o.state
                          FROM employees e
                          LEFT JOIN offices o ON e.officeCode=o.officeCode
                          ORDER BY e.firstName, e.lastName
""",conn)
df_employee

df_boston1 = pd.read_sql("""
SELECT * FROM customers
""",conn)
df_boston1

df_boston1 = pd.read_sql("""
SELECT * FROM orders
""",conn)
df_boston1

df_boston1 = pd.read_sql("""
SELECT * FROM orderdetails
""",conn)
df_boston1

# CodeGrade step4
df_no_orders = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
FROM customers c
LEFT JOIN orders o ON c.customerNumber = o.customerNumber
WHERE o.customerNumber IS NULL
ORDER BY c.contactLastName ASC
""", conn)
df_no_orders

df_boston1 = pd.read_sql("""
SELECT * FROM payments
""",conn)
df_boston1

# CodeGrade step5
df_payment = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, 
       CAST(p.amount AS DECIMAL) AS amount, 
       p.paymentDate
FROM customers c
LEFT JOIN payments p ON c.customerNumber = p.customerNumber
ORDER BY CAST(p.amount AS DECIMAL) DESC
""", conn)
df_payment

# CodeGrade step6
df_credit = pd.read_sql("""
SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) AS no_of_customers
FROM employees e
INNER JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber, e.firstName, e.lastName
HAVING AVG(c.creditLimit) > 90000
ORDER BY no_of_customers DESC
""", conn)
df_credit

# CodeGrade step7
# Replace None with your code
df_product_sold1 = pd.read_sql("""
SELECT * FROM products
""",conn)
df_product_sold1

df_product_sold = pd.read_sql("""
SELECT p.productName, COUNT(o.productCode) AS numorders, SUM(o.quantityOrdered) AS totalunits
FROM products p
INNER JOIN orderdetails o ON p.productCode = o.productCode
GROUP BY p.productCode, p.productName
ORDER BY totalunits DESC
""", conn)

# CodeGrade step8
# Replace None with your code
df_total_customers =  pd.read_sql("""
SELECT p.productName, p.productCode, COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM products p
INNER JOIN orderdetails od ON p.productCode = od.productCode
INNER JOIN orders o ON od.orderNumber = o.orderNumber
GROUP BY p.productCode, p.productName
ORDER BY numpurchasers DESC
""", conn)
df_total_customers

# CodeGrade step9
df_customers = pd.read_sql("""
SELECT o.officeCode, o.city, COUNT(c.customerNumber) AS n_customers
FROM offices o
INNER JOIN employees e ON o.officeCode = e.officeCode
INNER JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY o.officeCode, o.city
ORDER BY n_customers DESC
""", conn)
df_customers

df_codegrade8= pd.read_sql("""SELECT p.productCode
FROM products p
INNER JOIN orderdetails od ON p.productCode = od.productCode
INNER JOIN orders o ON od.orderNumber = o.orderNumber
GROUP BY p.productCode
HAVING COUNT(DISTINCT o.customerNumber) < 20
                           """,conn)
df_codegrade8

df_under_20 = pd.read_sql("""
SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
FROM employees e
INNER JOIN offices o ON e.officeCode = o.officeCode
INNER JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
INNER JOIN orders ord ON c.customerNumber = ord.customerNumber
INNER JOIN orderdetails od ON ord.orderNumber = od.orderNumber
WHERE od.productCode IN (
    SELECT p.productCode
    FROM products p
    INNER JOIN orderdetails od2 ON p.productCode = od2.productCode
    INNER JOIN orders o2 ON od2.orderNumber = o2.orderNumber
    GROUP BY p.productCode
    HAVING COUNT(DISTINCT o2.customerNumber) < 20
)
""", conn)

# Run this cell without changes

conn.close()