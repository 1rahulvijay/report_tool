import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import duckdb

sys.path.insert(0, os.path.abspath("c:/Users/rahul/OneDrive/Documents/Aurora/backend"))

from app.services.query_builder import QueryBuilderService
from app.schemas.query import QueryRequest

conn = duckdb.connect()
conn.execute(
    "CREATE TABLE employee_roster (id INTEGER, department VARCHAR, salary_usd DOUBLE)"
)
conn.execute(
    "INSERT INTO employee_roster VALUES (1, 'Sales', 100.0), (2, 'Books', 20.0), (3, 'Home', 50.0)"
)

req1 = {
    "dataset": "employee_roster",
    "columns": ["id", "amount"],
    "group_by": ["employee_roster.department"],
    "aggregations": [{"column": "salary_usd", "function": "sum", "output_name": "Sum"}],
}

qr = QueryRequest(**req1)
qb = QueryBuilderService()
sql, params = qb.build_query(qr)

print("SQL:", sql)
res = conn.execute(sql).df()

print("Columns returned in Dataframe:", list(res.columns))
