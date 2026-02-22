import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import duckdb

sys.path.insert(0, os.path.abspath("c:/Users/rahul/OneDrive/Documents/Aurora/backend"))

from app.services.query_builder import QueryBuilderService
from app.schemas.query import QueryRequest

# Create an in-memory db with a sample table
conn = duckdb.connect()
conn.execute("CREATE TABLE sales (id INTEGER, category VARCHAR, price DOUBLE)")
conn.execute(
    "INSERT INTO sales VALUES (1, 'Electronics', 100.0), (2, 'Books', 20.0), (3, 'Home', 50.0)"
)

# Test IN clause on varchar
req_varchar = {
    "dataset": "sales",
    "filters": {
        "logic": "AND",
        "conditions": [
            {"column": "category", "operator": "in", "value": "Electronics, Books"}
        ],
    },
}

try:
    qr = QueryRequest(**req_varchar)
    qb = QueryBuilderService()
    sql, params = qb.build_query(qr)
    print("Executing SQL on Varchar:", sql)
    print("Params:", params)
    res = conn.execute(sql, parameters=params).fetchall()
    print("Result:", res)
except Exception as e:
    print("Varchar IN Error:", e)

# Test IN clause on INTEGER
req_int = {
    "dataset": "sales",
    "filters": {
        "logic": "AND",
        "conditions": [{"column": "id", "operator": "in", "value": "1, 2"}],
    },
}

try:
    qr = QueryRequest(**req_int)
    qb = QueryBuilderService()
    sql, params = qb.build_query(qr)
    print("\nExecuting SQL on Integer:", sql)
    print("Params:", params)
    res = conn.execute(sql, parameters=params).fetchall()
    print("Result:", res)
except Exception as e:
    print("Integer IN Error:", e)
