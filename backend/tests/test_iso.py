import asyncio
import traceback
from app.db.factory import get_database_adapter
from app.api.endpoints import _parse_iso_dates
from datetime import datetime


async def test():
    db = get_database_adapter()

    # 1. Test EXACT SQL frontend sends
    sql1 = 'SELECT ORDER_DATE, SUM(REVENUE) AS TOTAL_REVENUE FROM AURORA_APP.GLOBAL_SALES_ORDERS WHERE "ORDER_DATE" IN (:__part_0) GROUP BY ORDER_DATE ORDER BY ORDER_DATE'
    sql2 = 'SELECT ORDER_DATE, SUM(REVENUE) AS TOTAL_REVENUE FROM AURORA_APP.GLOBAL_SALES_ORDERS WHERE "ORDER_DATE" IN (:part0) GROUP BY ORDER_DATE ORDER BY ORDER_DATE'

    raw_params1 = {"__part_0": "2023-12-31T00:00:00"}
    raw_params2 = {"part0": "2023-12-31T00:00:00"}

    parsed_params1 = _parse_iso_dates(raw_params1)
    parsed_params2 = _parse_iso_dates(raw_params2)

    print("\n--- Testing execute_query WITH UNDERSCORE ---")
    try:
        results = db.execute_query(sql1, parsed_params1)
        print("execute_query 1 SUCCESS!")
    except Exception as e:
        print(f"execute_query 1 FAILED: {type(e).__name__}: {e}")

    print("\n--- Testing execute_query WITHOUT UNDERSCORE ---")
    try:
        results = db.execute_query(sql2, parsed_params2)
        print("execute_query 2 SUCCESS!")
    except Exception as e:
        print(f"execute_query 2 FAILED: {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(test())
