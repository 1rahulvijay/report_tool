import httpx
import asyncio


async def test_pipeline():
    # Simulate TC-PIPE-01: Join -> Filter -> Aggregate
    payload = {
        "dataset": "employee_roster",
        "columns": [
            "employee_roster.department",
            "department_budgets.budget",
            "salary_sum",
        ],
        "joins": [
            {
                "join_type": "left",
                "left_dataset": "employee_roster",
                "right_dataset": "department_budgets",
                "on": [{"left_column": "department", "right_column": "dept_name"}],
            }
        ],
        "filters": {
            "logic": "AND",
            "conditions": [
                {
                    "column": "employee_roster.department",
                    "operator": "eq",
                    "value": "Sales",
                },
                {"column": "salary_sum", "operator": "gt", "value": "1000"},
            ],
        },
        "group_by": ["employee_roster.department", "department_budgets.budget"],
        "aggregations": [
            {
                "column": "employee_roster.salary_usd",
                "function": "sum",
                "output_name": "salary_sum",
            }
        ],
        "limit": 100,
        "offset": 0,
        "use_high_perf_hints": False,
        "is_virtual_scroll": False,
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
            print("Total rows:", data.get("total_row_count"))
            print("Data preview:", data.get("data")[:2] if data.get("data") else [])


if __name__ == "__main__":
    asyncio.run(test_pipeline())
