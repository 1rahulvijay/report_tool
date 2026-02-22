import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import httpx
import asyncio


# Testing the FastAPI directly via httpx locally
async def run_test():
    payload = {
        "dataset": "employee_roster",
        "columns": ["EMPLOYEE_ROSTER.SALARY_USD", "SALARY_SUM"],
        "joins": [],
        "limit": 50,
        "offset": 0,
        "filters": {
            "logic": "AND",
            "conditions": [{"column": "salary_sum", "operator": "eq", "value": "0"}],
        },
        "group_by": ["employee_roster.salary_usd"],
        "aggregations": [
            {
                "column": "employee_roster.salary_usd",
                "function": "sum",
                "output_name": "salary_sum",
            }
        ],
        "use_high_perf_hints": False,
        "is_virtual_scroll": False,
    }

    try:
        async with httpx.AsyncClient() as client:
            res = await client.post(
                "http://localhost:8080/api/v1/query/preview", json=payload, timeout=5.0
            )
            print("Status Code:", res.status_code)
            if res.status_code != 200:
                print("Error Details:", res.text)
            else:
                data = res.json()
                print("Total rows:", data.get("total_row_count"))
                print("Data length:", len(data.get("data", [])))
    except Exception as e:
        print("Got exception hitting API:", e)


if __name__ == "__main__":
    asyncio.run(run_test())
