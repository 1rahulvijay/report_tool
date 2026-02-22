import asyncio
import httpx


async def test_derived_filter():
    payload = {
        "dataset": "employee_roster",
        "columns": ["employee_roster.department", "salary_sum"],
        "filters": {
            "logic": "AND",
            "conditions": [
                {"column": "salary_sum", "operator": "gt", "value": "35000000"}
            ],
        },
        "group_by": ["employee_roster.department"],
        "aggregations": [
            {
                "column": "employee_roster.salary_usd",
                "function": "sum",
                "output_name": "salary_sum",
            }
        ],
        "limit": 100,
        "offset": 0,
    }

    async with httpx.AsyncClient() as client:
        res = await client.post(
            "http://localhost:8080/api/v1/query/preview", json=payload, timeout=10.0
        )
        print("Status Code:", res.status_code)
        if res.status_code != 200:
            print("Error Details:", res.text)
        else:
            data = res.json()
            print("Row Count:", data.get("total_row_count"))
            print("Exec Time:", data.get("execution_time_ms"))
            print("Data:", data.get("data")[:2])


if __name__ == "__main__":
    asyncio.run(test_derived_filter())
