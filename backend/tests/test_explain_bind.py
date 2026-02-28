import asyncio
from app.db.factory import get_database_adapter
from datetime import datetime
import pandas as pd


async def test():
    db = get_database_adapter()
    sql = 'SELECT ORDER_DATE, REVENUE FROM AURORA_APP.GLOBAL_SALES_ORDERS WHERE "ORDER_DATE" IN (:__part_0) FETCH NEXT 5 ROWS ONLY'

    # Try as string directly via execute_query (Wait, DPY-4008 happens if bind is mismatched)
    # Let's try executing the explain plan
    print("Testing explain_query with datetime bind...")
    try:
        params = {"__part_0": datetime(2023, 12, 31, 0, 0, 0)}
        db.explain_query(sql, params)
        print("Explain success!")
    except Exception as e:
        print(f"Explain failed: {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(test())
