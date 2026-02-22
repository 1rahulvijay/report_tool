import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Add backend dir to pythonpath
sys.path.insert(0, os.path.abspath("c:/Users/rahul/OneDrive/Documents/Aurora/backend"))

from app.services.query_builder import QueryBuilderService
from app.schemas.query import QueryRequest

# Test: aggregation with group by
req1 = {
    "dataset": "sales",
    "columns": ["id", "amount"],
    "group_by": ["department"],
    "aggregations": [{"column": "salary_usd", "function": "sum", "output_name": "Sum"}],
}

qr = QueryRequest(**req1)
qb = QueryBuilderService()
sql, params = qb.build_query(qr)
print("SQL:\n", sql)
