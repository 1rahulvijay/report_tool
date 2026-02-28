import asyncio
from app.db.factory import get_database_adapter
from datetime import datetime
import pandas as pd


async def test():
    db = get_database_adapter()
    sql = 'SELECT ORDER_DATE, REVENUE FROM AURORA_APP.GLOBAL_SALES_ORDERS WHERE "ORDER_DATE" IN (:__part_0) FETCH NEXT 5 ROWS ONLY'

    # Try as string
    try:
        print("Testing String Bind...")
        params = {"__part_0": "2023-12-31T00:00:00"}
        df = db.execute_query(sql, params)
        print("String success!")
    except Exception as e:
        print(f"String failed: {e}")

    # Try as datetime
    try:
        print("Testing Datetime Bind...")
        params = {"__part_0": datetime(2023, 12, 31, 0, 0, 0)}
        df = db.execute_query(sql, params)
        print("Datetime success!")
    except Exception as e:
        print(f"Datetime failed: {e}")

    # Try TO_DATE explicit cast
    sql2 = 'SELECT ORDER_DATE, REVENUE FROM AURORA_APP.GLOBAL_SALES_ORDERS WHERE "ORDER_DATE" IN (TO_DATE(:__part_0, \'YYYY-MM-DD"T"HH24:MI:SS\')) FETCH NEXT 5 ROWS ONLY'
    try:
        print("Testing Explicit TO_DATE Bind...")
        params = {"__part_0": "2023-12-31T00:00:00"}
        df = db.execute_query(sql2, params)
        print("TO_DATE success!")
    except Exception as e:
        print(f"TO_DATE failed: {e}")


if __name__ == "__main__":
    asyncio.run(test())
