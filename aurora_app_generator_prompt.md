# Aurora Builder Prompt

You are an expert full-stack developer specializing in Python, FastAPI, and Reflex (React/Next.js). Your task is to build **Aurora**, a Self-Service Reporting Portal from scratch.

## Project Overview
Aurora is a modern, highly interactive web application that empowers non-technical business users to explore datasets, apply advanced filters, execute multi-table joins, perform data aggregations, and export production-ready reports without writing SQL. It bridges raw enterprise data warehouses with business users using an "Excel-like" experience.

## Tech Stack
*   **Frontend**: Reflex (Python framework compiling to Next.js/React).
*   **Backend**: FastAPI (Python) for high-performance, asynchronous REST APIs.
*   **Database Integration**: An abstract `DatabaseAdapter` interface. Initially, implement a `DuckDBAdapter` for local, in-memory testing (CSVs, Parquet). The architecture must seamlessly swap to an `OracleAdapter` using `oracledb` for production execution.
*   **Proxy/Deployment**: Nginx routing `/api/v1` to FastAPI and `/_event` to the Reflex websocket server.

## Core Backend Features (FastAPI)
1. **Dynamic Metadata Discovery**: Endpoints to list available datasets and fetch their column metadata (data types, etc.).
2. **Query Engine (`QueryBuilderService`)**:
    *   Accepts a JSON payload defining the dataset, visible columns, filters, joins, aggregations, and sorting.
    *   Translates the payload into parameterized SQL safely.
    *   Supports logical trees (AND/OR groups).
    *   Supports operators: `=`, `!=`, `<`, `>`, `<=`, `>=`, `contains`, `starts with`, `ends with`, `in`, `between`, `is null`, `is empty`.
    *   Must use explicit **Predicate Pushdown**: filter base tables *before* applying `JOIN` clauses.
    *   **Safeguards**: Never execute `SELECT *`. Must always wrap preview queries with `LIMIT` (e.g., `OFFSET 0 LIMIT 50`).
3. **Connection Pooling & Concurrency**:
    *   Use `oracledb.create_pool` with `min=50`, `max=150`, and `wait_timeout=5000`.
    *   Implement retry logic on connection acquisition failures.
    *   Run executed queries in `asyncio.to_thread` with timeouts (e.g., 30s) to free the event loop.
4. **Rate Limiting**: Use `slowapi` to enforce limits (e.g., `300/minute` per user via a custom `X-User-ID` header fallback to IP).
5. **Export Pipeline**:
    *   Background Excel Generation: Use `ThreadPoolExecutor` (max 16 workers) to write `.xlsx` files using `xlsxwriter` in `constant_memory=True` mode.
    *   Support CSV streaming.
    *   Enforce Row Governance: Reject Excel exports over a configurable `MAX_ROWS` (e.g., 100k).

## Core Frontend Features (Reflex UI)
1. **State Management (`AppState`)**:
    *   Strictly separate UI State (modals open/close) from Data State (the generated JSON query payload).
    *   The UI must act as a "dumb terminal" holding only the current page (e.g., 50 rows) to prevent out-of-memory errors on the server.
2. **The Datagrid**:
    *   A high-performance tabular viewer rendering the current page of data.
    *   Support Virtual Scrolling or Paginated viewing.
    *   Sticky headers and checkbox selection.
3. **Advanced Filters Modal**:
    *   A drag-and-drop or structured UI to build nested AND/OR condition groups.
    *   Dynamically update available operators based on the selected column's datatype (string, number, date).
    *   Handle "between" operators with comma-separated or "to" delimited strings, splitting them appropriately before sending to the backend.
4. **Data Vintage / Partition Bar**:
    *   A top-level control to select `Load Type` (e.g., Daily/Monthly) and `Load ID` epochs.
    *   This explicitly sets partition filters that the backend injects *before* anything else to leverage database partition pruning.
5. **Join Builder Modal**:
    *   Visual interface to define `Primary` and `Secondary` tables.
    *   Select `INNER`, `LEFT`, `RIGHT`, or `FULL OUTER` joins.
    *   Define multiple `ON` conditions mapping Left Column = Right Column.
6. **Aggregation Builder**:
    *   Select "Group By" columns.
    *   Define measures using `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, `DISTINCT_COUNT`.
    *   When applied, the app must dynamically morph the visible schema so subsequent filters apply to the aggregated aliases via `HAVING` or subqueries.
7. **UI Design System**:
    *   Ultra-compact enterprise design using Tailwind CSS classes.
    *   Use navy, slate, and primary blue colors.
    *   Modals must not bleed or overflow laterally (use Flexbox `min-w-0`, `flex-1`, `overflow-auto`).
    *   Use pill-shaped toggles and inputs.

## Important Constraints to Follow
*   **Type Casting**: The backend MUST actively look up the `base_type` of a column from metadata when mapping "Partition Filters" and cast URL strings into `int`, `float`, or Python `datetime` objects before binding them.
*   **Deep Copies**: All mutations to lists/dicts within the Reflex `AppState` must use `copy.deepcopy()` to avoid mutating shared pointers.
*   **Security**: Use bind parameters (`:p_1`, `:p_2`) for everything. NEVER concatenate strings directly into the WHERE or JOIN clauses.
*   **Cost Interception**: The backend `OracleAdapter` should implement an `explain_query` method to run `EXPLAIN PLAN`. If the cost is too high, raise a 400 error requiring the user to add partition constraints.

Begin by scaffolding the FastAPI backend structure, defining the Pydantic schemas, and setting up the Database Adapter interface. Then, proceed to build the Reflex frontend state management and UI components.
