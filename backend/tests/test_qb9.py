import duckdb

conn = duckdb.connect()
conn.execute(
    "CREATE TABLE employee_roster (id INTEGER, department VARCHAR, salary_usd DOUBLE)"
)
conn.execute(
    "INSERT INTO employee_roster VALUES (1, 'Sales', 100.0), (2, 'Books', 20.0), (3, 'Home', 50.0), (4, 'Sales', 200.0)"
)

# Test HAVING with string param vs float column
try:
    sql = """
    SELECT department, SUM(salary_usd) AS Sum
    FROM employee_roster
    GROUP BY department
    HAVING Sum <= ?
    """
    res = conn.execute(sql, ["20"]).df()
    print("HAVING test with string parameter:")
    print(res)
except Exception as e:
    print("Error:", e)

# Test what happens if the parameter is correctly cast to a float
try:
    sql = """
    SELECT department, SUM(salary_usd) AS Sum
    FROM employee_roster
    GROUP BY department
    HAVING Sum <= ?
    """
    res = conn.execute(sql, [20.0]).df()
    print("\nHAVING test with float parameter:")
    print(res)
except Exception as e:
    print("Error:", e)
