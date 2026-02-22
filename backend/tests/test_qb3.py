import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

sys.path.insert(0, os.path.abspath("c:/Users/rahul/OneDrive/Documents/Aurora/backend"))

from app.services.query_builder import QueryBuilderService
from app.schemas.query import QueryRequest

# Test in operator with a single value
req2 = {
    "dataset": "sales",
    "columns": ["id", "amount"],
    "filters": {
        "logic": "AND",
        "conditions": [
            {"column": "category", "operator": "in", "value": "Electronics"}
        ],
    },
}

qr = QueryRequest(**req2)
qb = QueryBuilderService()
sql, params = qb.build_query(qr)
print("Single value IN", sql, params)
