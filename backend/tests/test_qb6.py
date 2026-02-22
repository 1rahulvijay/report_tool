import duckdb

conn = duckdb.connect()
conn.execute(
    "CREATE TABLE employee_roster (id INTEGER, department VARCHAR, salary_usd DOUBLE)"
)
conn.execute(
    "INSERT INTO employee_roster VALUES (1, 'Sales', 100.0), (2, 'Books', 20.0), (3, 'Home', 50.0), (4, 'Sales', 200.0)"
)

# Test HAVING with alias
sql = """
SELECT department, SUM(salary_usd) AS Sum
FROM employee_roster
GROUP BY department
HAVING Sum > 100
"""
res = conn.execute(sql).df()
print("HAVING test:")
print(res)

# Test duckdb boolean logic on aliases
sql2 = """
SELECT department, SUM(salary_usd) AS Sum
FROM employee_roster
WHERE department = 'Sales'
GROUP BY department
HAVING Sum > 100
"""
res2 = conn.execute(sql2).df()
print("\nWHERE + HAVING test:")
print(res2)
