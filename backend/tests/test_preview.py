import urllib.request
import urllib.error
import json

payload = {
    "dataset": "GLOBAL_SALES_ORDERS",
    "columns": ["ORDER_ID", "CUSTOMER_NAME"],
    "limit": 50,
    "offset": 0,
}

req = urllib.request.Request(
    "http://localhost:8080/api/v1/query/preview",
    data=json.dumps(payload).encode("utf-8"),
    headers={"Content-Type": "application/json"},
)

try:
    res = urllib.request.urlopen(req)
    print("SUCCESS:")
    print(res.read().decode("utf-8"))
except urllib.error.HTTPError as e:
    print("HTTP ERROR:", e.code)
    print(e.read().decode("utf-8"))
except Exception as e:
    print("OTHER ERROR:", str(e))
