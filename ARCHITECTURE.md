# Aurora Self-Service Reporting Portal: Architecture & System Design

## 1. Project Goal
The primary objective of the Aurora Self-Service Reporting Portal is to provide a modern, highly interactive, and intuitive web application for business users. It empowers non-technical teams to quickly explore datasets, apply advanced filters, execute multi-table joins, perform complex data aggregations, and export production-ready reports—all without writing a single line of SQL.

It serves as a seamless bridge between raw enterprise data warehouses and non-technical business users, emphasizing a clean, "Excel-like" user experience with a focus on progressive disclosure, discoverability, and high performance.

## 2. System Architecture & Tech Stack
The application follows a decoupled client-server architecture, highly optimized for analytical processing.

- **Frontend (UI & State Management):** Built with **Reflex** (Python). Reflex translates Python-defined UI components into a reactive Next.js/React web application under the hood. Complex UI state (like building queries, managing joining logic, and handling modals) is managed centrally via `AppState`.
- **Backend (API & Query Engine Integration):** Built with **FastAPI** (Python). It provides high-performance, asynchronous RESTful endpoints. The backend handles fetching metadata, previewing chunks of data, dynamically building SQL, executing queries securely, and formatting exports.
- **Database Engine & Execution Layer:** 
  The backend implements a highly adaptable `DatabaseAdapter` interface to execute dynamically generated SQL queries securely.
  
## 3. The Oracle Plugin & DuckDB Workaround
A critical component of this system's architecture is its database abstraction strategy:
- **The "DuckDB Workaround" (Current Setup):** To ensure rapid prototyping, seamless local development, and zero-dependency testing, the application currently uses **DuckDB**, an embedded, blazing-fast analytical database engine. The `DuckDBAdapter` processes local data arrays, CSVs, or Parquet files entirely in-memory. DuckDB serves as a highly capable and localized stand-in.
- **The Oracle Plugin Target (Production Goal):** The architecture is explicitly designed so that migrating to an enterprise environment requires zero logic changes to the frontend or the query generation core. By swapping the `DuckDBAdapter` for an `OracleAdapter` (the "plugin"), the FastAPI backend will seamlessly route the exact same generated dimensional SQL queries directly against a production Oracle data warehouse.

## 4. Core Features & Capabilities
### The Datagrid Explorer
- High-performance tabular viewer with real-time column visibility toggling.
- Native float precision formatting (rounding monetary/double values rigidly to 2 decimal places to prevent float overflow).
- Client-side data search, sorting, and pagination.

### Advanced Filtering (Query Generation)
- A powerful structural interface strictly supporting nested Logical Groups (AND/OR trees).
- Support for complex condition operators (`=`, `!=`, `<`, `>`, `<=`, `>=`, `contains`, `starts with`, `ends with`, `in`, `between` for strict Date Range constraints).
- Handlers elegantly map comma-separated lists from the UI directly into robust `IN (...)` and `BETWEEN` parameter arrays.
- **Dynamic Post-Aggregation Schema:** Determines if a user is filtering raw data (using robust `WHERE` clauses) or derived/aggregated data (seamlessly routing to `.HAVING()` clauses or subqueries transparently).

### Data Vintage & Load Constraints
- Implements a dedicated "Data Vintage" UI dropdown natively integrated atop partitioned datasets.
- Rapidly slices data by `Load Type` and distinct `Load ID` epochs, accelerating partition pruning natively inside the Data Warehouse layer *before* heavy joins or filters apply.

### Aggregation Builder
- Enables the definition of custom report granularity via **Group-By Columns**.
- Supports defining measures via standard SQL functions (`SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, `DISTINCT_COUNT`).
- Dynamically mutates the visible schema inside the UI so downstream features (like filters or exporting) only operate against the newly aggregated aliases.

### Data Join Builder
- Visual configuration for merging multiple datasets.
- Support for defining the Primary (Left) and Secondary (Right) tables.
- Support for explicit `INNER`, `LEFT`, `RIGHT`, and `FULL OUTER` joins.
- Live sample previews of the joined result schema before applying constraints to the main environment.

### The Export Pipeline
- Allows business users to extract data subsets tailored to their strict requirements.
- Converts JSON payloads directly to **CSV** or **Excel (XLSX)**.
- **Safety & Performance:** Uses `xlsxwriter` in `constant_memory` mode alongside strict row execution limits (e.g., controlled by environment variables like `EXPORT_EXCEL_MAX_ROWS` and `MAX_ROW_LIMIT`) to rigorously defend the backend FastAPI service against Memory Out-Of-Bounds failures.

## 5. UI/UX Design System
- **Ultra-Compact Dense Interfaces:** Tailored explicitly for enterprise reporting. Employs aggressively minimal whitespace, compact paddings (`p-3`, `p-4`), and standardized header formatting to display maximum data and prevent excessive scrolling.
- **Strict Overlap Constraints:** Modals (Aggregations, Filters, Joins) adhere strictly to CSS Flexbox layouts with maximum bound wrappers (`max-h-[90vh]`, `flex-1`, `min-w-0`), absolutely prohibiting horizontal bleed or vertical cutoff, even as users inject infinite rules.
- **Premium Aesthetics:** Relies on a crisp Navy, Slate, and Bright Blue enterprise color palette. Employs rounded pill inputs, pill-shaped tags for group-by operations, and clean Lucide/Material icons to denote interaction status.
- **Zero-Friction Global Resets:** A central "Reset All" action cleans up complex states (wiping joins, aggregations, schema morphs, and filters simultaneously), acting as a powerful fail-safe for lost users.

## 6. Scaling to 200 Concurrent Users (Prototype to Production)
Optimizing analytical workloads (complex joins, multi-level aggregations, large exports) is the single most critical challenge for scaling this architecture to 200 concurrent users. To protect the FastAPI backend and databases (DuckDB and eventually Oracle), the system must implement strict safeguards across three distinct layers.

### 6.1. The Query Generation Layer (FastAPI)
The backend acts as a strict translator and optimizer, rather than a naive SQL concatenator.

- **Aggressive Column Pruning:** The query builder strictly inspects the "Visible Columns" array from the Reflex UI and only requests those specific columns. Queries like `SELECT *` are prohibited to prevent massive memory payloads.
- **Predicate Pushdown (Filter Before Joining):** When joining a massive table with an established filter (e.g., `department = 'Sales'`), the base table is filtered in a Common Table Expression (CTE) or subquery *before* the join occurs.
- **Smart Join Order Calculation:** For multi-table operations, the query builder orders the SQL so the smallest, most heavily filtered table drives the join paths.

### 6.2. Execution Safeguards (Reflex UI & FastAPI)
Guardrails to prevent user-generated cartesian products from monopolizing server RAM or CPU.

- **Strict Environment Row Constraints:** Heavy aggregate counts and deep `OFFSET` pagination layers are guarded by an overriding, environment-configurable `MAX_ROW_LIMIT`.
- **Lazy Initialization (No Default Table):** Explicitly enforces an unselected database view upon application initialization. The system strictly waits for a human interaction before generating query events, protecting the backend from catastrophic "stampede" failures on simultaneous client startup.
- **Reflex State Management (The Memory Trap):** Reflex manages state server-side via WebSockets. To prevent memory exhaustion at 200 users, the UI acts strictly as a "dumb terminal." It only holds the currently visible page of data (e.g., 50 rows) and the JSON query configuration. Heavy lifting, sorting, and full dataset retention are pushed down to the FastAPI and database layers.
- **The "Preview" Protection:** UI actions like "View Example Data" append strict limiters (`LIMIT 50` for DuckDB, `FETCH FIRST 50 ROWS ONLY` for Oracle). The system never executes a full multi-million row join just to populate a schema preview.
- **Pagination Pushdown:** The Reflex datagrid only asks for `page=1, size=50`. FastAPI maps this to `OFFSET 0 LIMIT 50` in the database engine, avoiding massive payloads over the network.
- **Timeout & Cost Estimation Limits:** Before executing on Oracle, FastAPI runs an `EXPLAIN PLAN`. If the database optimizer estimates an astronomical cost, FastAPI intercepts and blocks the execution, returning a friendly error asking the user to apply a date, department, or Load ID partition filter first.

### 6.3. Database-Level Tuning (Oracle Plugin Preparation)
Transitioning from the DuckDB single-node bottleneck to enterprise-scale concurrency requires explicit tuning on the Oracle database itself.

- **FastAPI & Connection Pooling:** As the `OracleAdapter` replaces DuckDB, the backend incorporates robust connection pooling (using SQLAlchemy or `oracledb`). Proper configuration of min/max pool sizes ensures queries queue gracefully under burst loads.
- **Foreign Key & Indexing Discipline:** Oracle's optimizer relies on statistics to choose between Hash, Merge, or Nested Loop joins. All columns exposed in the UI's "Join Conditions" must be backed by B-Tree or Bitmap indexes.
- **Star Schema Pre-computations (Materialized Views):** For highly frequent join patterns (e.g., `employee_roster` + `global_sales`), Oracle is configured with Nightly/Hourly Materialized Views. The FastAPI builder is trained to transparently route users to these views instead of recalculating the raw join 200 times a minute.
- **Partitioning:** Large transactional tables are partitioned by Date. The UI is configured to enforce a Date filter constraint, enabling Oracle Partition Pruning to skip scanning 90% of the table.

### 6.4. The Export Pipeline Resilience
- **Current Safeguard:** Utilizing `xlsxwriter` in `constant_memory` mode with strict execution limits (e.g., 50,000 maximum rows) defends the FastAPI service against Memory Out-Of-Bounds failures.
- **Future Safeguard:** At 200 concurrent users, maximum-capacity exports will eventually be offloaded to a background task queue (like Celery or ARQ). This prevents massive I/O operations from blocking the FastAPI event loop for users performing simple filter toggles.
