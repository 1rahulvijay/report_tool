import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Add backend dir to pythonpath
sys.path.insert(0, os.path.abspath("c:/Users/rahul/OneDrive/Documents/Aurora/backend"))

from app.services.query_builder import QueryBuilderService
from app.schemas.query import QueryRequest

# Test 1: aggregation without output_name
req1 = {
    "dataset": "sales",
    "columns": ["id", "amount"],
    "aggregations": [
        {"column": "price", "function": "sum", "output_name": ""}
    ]
}

try:
    qr = QueryRequest(**req1)
    qb = QueryBuilderService()
    sql, params = qb.build_query(qr)
    print("Test 1 SUCCESS:")
    print("SQL:", sql)
    print("PARAMS:", params)
except Exception as e:
    print("Test 1 FAILED:", getattr(e, 'errors', lambda: str(e))())

# Test 2: in clause filter
req2 = {
    "dataset": "sales",
    "columns": ["id", "amount"],
    "filters": {
        "logic": "AND",
        "conditions": [
            {
                "column": "category",
                "operator": "in",
                "value": "Electronics, Books"
            }
        ]
    }
}

try:
    qr = QueryRequest(**req2)
    qb = QueryBuilderService()
    sql, params = qb.build_query(qr)
    print("\nTest 2 SUCCESS:")
    print("SQL:", sql)
    print("PARAMS:", params)
except Exception as e:
    print("\nTest 2 FAILED:", getattr(e, 'errors', lambda: str(e))())
