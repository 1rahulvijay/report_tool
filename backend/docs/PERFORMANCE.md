# Aurora Reporting Engine - Performance & Scaling Guide

This document outlines the strategy and configuration required to support **~200 concurrent users** on the Aurora Reporting Engine using a **Strictly Streaming** architecture (zero container persistence).

## 1. Concurrent User Capacity Formula

To support 200 concurrent users without external brokers (Redis/Celery), we use a synchronous memory-based model:

-   **Active HTTP Requests**: Estimated 10% of concurrent users are actively hitting the API/Export at any given second (20 req/s).
-   **Database Connections**: Each active request holds 1 connection during data retrieval.
-   **Memory-Based Exports**: XLSX/CSV generation happens entirely in RAM and streams directly to the client.

### Database Pool Size Calculation

> [!IMPORTANT]
> The database pool is **per-process**. Total connections = `API_PROCESSES × ORACLE_MAX_POOL`.

- **Recommended Sizing (200 Users)**:
  - `API_PROCESSES`: 4-6 (Gunicorn workers)
  - `ORACLE_MAX_POOL`: 20-30
  - **Total Connections**: 80-120 (Safe for standard Oracle DB instances)
  - `QUERY_TIMEOUT_SECONDS`: 120

### Safe Global Connection Count

Use this formula to calculate the maximum number of DB connections your deployment will open:

```
Safe Global Connections = API_PROCESSES × ORACLE_MAX_POOL
```

| Deployment Size | Workers | `ORACLE_MAX_POOL` | Total Connections | Oracle Limit |
|:---|:---|:---|:---|:---|
| **Dev / Local** | 1 | 10 | **10** | 150 (default) |
| **Small (50 users)** | 2 | 15 | **30** | 150 |
| **Medium (200 users)** | 4 | 20 | **80** | 300 |
| **Large (500 users)** | 8 | 25 | **200** | 500+ |

> [!WARNING]
> If `Total Connections` exceeds your Oracle instance's `SESSIONS` parameter, new connections will fail. Check with: `SELECT value FROM v$parameter WHERE name = 'sessions'`.

## 2. Pool Exhaustion — 503 Fail-Fast

When all connections in the pool are busy and the `wait_timeout` (2s) expires:

1. The `OracleAdapter.connection()` context manager raises `ValueError("DATABASE_POOL_EXHAUSTED")`.
2. The global FastAPI exception handler converts this to **HTTP 503 Service Unavailable** with `Retry-After: 5` header.
3. The frontend receives the 503 and can display a retry message to the user.

This prevents request queuing and cascading failures under load.

## 3. Resource Constraints

| Component | Limit | Strategy |
| :--- | :--- | :--- |
| **Memory** | 6GB - 12GB | Higher RAM required to buffer in-memory Excel generation. |
| **CPU** | 4 - 8 Cores | Parallel processing for 200 users. |
| **Temp Storage** | 0 GB | **Zero requirement**. No files are ever saved in the container. |

## 4. Data Governance Enforcements

| Metric | Limit | Action |
| :--- | :--- | :--- |
| **Preview Row Limit** | 500 rows | Truncates results, forces use of filters. |
| **Excel Export (XLSX)** | ≤ 100,000 rows | Generated in-memory and streamed. Larger sets rejected for RAM safety. |
| **CSV Streaming** | Unlimited | Pushed directly from DB cursor to HTTP stream (Memory Efficient). |

## 5. Rate Limiting Strategy

1. **Nginx Level**: Global hard limits (10 req/s per IP) to prevent worker saturation.
2. **App Level (SlowAPI)**:
   - `PREVIEW_RATE_LIMIT=60/min`: Prevents "button mashing".
   - `EXPORT_RATE_LIMIT=5/min`: Bounded by peak RAM availability.
3. **Concurrency Guard**: Max 2 concurrent analytical tasks (Previews/Exports) per user.

## 6. Environment Variables Reference

| Variable | Default | Description |
|:---|:---|:---|
| `ORACLE_MIN_POOL` | 2 | Min connections kept alive in pool |
| `ORACLE_MAX_POOL` | 10 | Max connections per process |
| `PREVIEW_MAX_ROWS` | 500 | Max rows returned for preview queries |
| `PREVIEW_RATE_LIMIT` | 60/minute | Rate limit for preview requests |
| `EXPORT_RATE_LIMIT` | 5/minute | Rate limit for export requests |
| `EXPORT_QUEUE_MAX` | 50 | Max concurrent export queue depth |
| `QUERY_TIMEOUT_SECONDS` | 60 | SQL execution timeout |
| `EXPORT_EXCEL_MAX_ROWS` | 100,000 | Hard limit for in-memory Excel |
| `EXPLAIN_PLAN_THRESHOLD` | 1,000,000 | Max cardinality before query rejection |

## 7. Observability

Monitor health via the `/metrics` endpoint:
- `db_pool_busy_count`: If > 80% of `MAX_POOL`, increase pool size.
- `db_pool_wait_count`: If > 0, requests are queuing for DB connections.

## 8. Infrastructure Recommendation

For **200 Users**:
- **Backend Process**: Gunicorn with 4-6 Uvicorn workers.
- **Nginx**: Proxy buffering disabled/tuned for large streams.
- **Zero Disk**: Ensure no write volumes are attached to the API container.
