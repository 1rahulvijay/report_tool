import duckdb

conn = duckdb.connect()
conn.execute("CREATE TABLE employee_roster (id INTEGER, salary_usd DOUBLE)")
conn.execute("INSERT INTO employee_roster VALUES (1, 105500.05)")

# Test HAVING with string param vs float column
try:
    sql = """
    SELECT salary_usd, SUM(salary_usd) AS salary_sum
    FROM employee_roster
    GROUP BY salary_usd
    HAVING salary_sum = ?
    """
    res = conn.execute(sql, ["0"]).df()
    print("HAVING test with string parameter = '0':")
    print(res)
except Exception as e:
    print("Error:", e)
