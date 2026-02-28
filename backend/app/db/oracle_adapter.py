import oracledb
from typing import Any, Dict, List, Optional
import pandas as pd
from datetime import datetime, timezone
import contextlib

from .base import BaseDatabaseAdapter
from app.core.logger import logger


class OracleAdapter(BaseDatabaseAdapter):
    """
    Enterprise Oracle implementation of the database adapter.
    Supports connection pooling and handles large metadata discovery.
    """

    def __init__(
        self, user: str, password: str, dsn: str, min_pool: int = 5, max_pool: int = 20
    ):
        self._user = user.upper()
        self.pool = oracledb.create_pool(
            user=user,
            password=password,
            dsn=dsn,
            min=min_pool,
            max=max_pool,
            increment=1,
            wait_timeout=2000,  # Fail fast (2s) if pool is exhausted
        )
        self._cache = {}
        self._cache_ttl = 3600  # 1 hour

    def _parse_dataset_name(self, dataset_name: str):
        """
        Splits a potentially schema-qualified dataset name into (owner, table).
        'MGBCM.REAL_DATA_1' -> ('MGBCM', 'REAL_DATA_1')
        'employee_roster'   -> (self._user, 'EMPLOYEE_ROSTER')

        Supports logical-to-physical mapping via table_config.
        """
        from app.core.table_config import resolve_physical_name

        physical_name = resolve_physical_name(dataset_name)

        if "." in physical_name:
            parts = physical_name.split(".", 1)
            return parts[0].upper(), parts[1].upper()
        return self._user, physical_name.upper()

    def _qualified_table(self, owner: str, table: str) -> str:
        """Returns a quoted schema-qualified table reference: \"OWNER\".\"TABLE\"."""
        return f'"{owner}"."{table}"'

    @contextlib.contextmanager
    def connection(self):
        """Safe connection context manager with auto-release and retry logic."""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                # Add wait_timeout to fail fast if pool is exhausted
                conn = self.pool.acquire()
            except oracledb.DatabaseError as e:
                error_obj = e.args[0]
                # ORA-12541: TNS:no listener, or ORA-12170: TNS:Connect timeout
                # ORA-12537: TNS:connection closed, etc.
                if isinstance(error_obj, oracledb.DatabaseError) or (
                    hasattr(error_obj, "code")
                    and error_obj.code in (12541, 12170, 12537, 28759)
                ):
                    raise RuntimeError(
                        f"Oracle Database is unreachable: {str(e)}"
                    ) from e

                # Check for pool timeout specifically (DPY-6001 or pool exhausted)
                if "DPY-6001" in str(e) or "pool exhausted" in str(e).lower():
                    # Fast 503-style error for pool exhaustion
                    raise ValueError(
                        "DATABASE_POOL_EXHAUSTED: All available connections are in use. Please try again in a moment."
                    )

                # Re-raise generic DB errors
                raise e

            # Yield connection and release it properly. Exceptions occurring inside
            # the yield (query execution) will propagate through seamlessly.
            try:
                from app.core.logger import logger

                logger.debug("Acquired connection from pool")
                yield conn
            finally:
                self.pool.release(conn)
                logger.debug("Released connection back to pool")
            return

    def get_datasets(self) -> List[Dict[str, Any]]:
        """
        Dynamically discover available tables and views across schemas.
        Returns dataset names in OWNER.TABLE_NAME format for multi-schema support.
        Includes row counts from NUM_ROWS (approximate for speed).
        """
        import time
        from app.core.config import get_settings

        now = time.time()
        if "datasets" in self._cache:
            cached_obj, cached_time = self._cache["datasets"]
            if now - cached_time < self._cache_ttl:
                return cached_obj

        settings = get_settings()

        table_filter = ""
        view_filter = ""
        params = {}
        if settings.ORACLE_SCHEMA_FILTER:
            # Support comma-separated schema list: "AURORA_APP,MGBCM,MGLOBE"
            schemas = [
                s.strip().upper()
                for s in settings.ORACLE_SCHEMA_FILTER.split(",")
                if s.strip()
            ]
            if len(schemas) == 1:
                table_filter = "WHERE owner = :owner_0"
                view_filter = "WHERE owner = :owner_0"
                params["owner_0"] = schemas[0]
            else:
                owner_placeholders = ", ".join(
                    f":owner_{i}" for i in range(len(schemas))
                )
                for i, s in enumerate(schemas):
                    params[f"owner_{i}"] = s
                table_filter = f"WHERE owner IN ({owner_placeholders})"
                view_filter = f"WHERE owner IN ({owner_placeholders})"
        else:
            sys_owners = "('SYS', 'SYSTEM', 'XDB', 'CTXSYS', 'MDSYS', 'ORDSYS', 'OUTLN', 'WMSYS', 'APPQOSSYS', 'DBSNMP', 'OJVMSYS', 'DVSYS', 'LBACSYS', 'AUDSYS', 'GSMADMIN_INTERNAL', 'ORACLE_OCM', 'PUBLIC', 'SYSMAN', 'EXFSYS', 'DIP', 'ANONYMOUS', 'XS$NULL', 'OACSYS')"
            table_filter = f"WHERE owner NOT IN {sys_owners} AND table_name NOT LIKE 'ALL$%' AND table_name NOT LIKE 'ALL\\_%' ESCAPE '\\' AND table_name NOT LIKE 'DBA\\_%' ESCAPE '\\' AND table_name NOT LIKE 'USER\\_%' ESCAPE '\\'"
            view_filter = f"WHERE owner NOT IN {sys_owners} AND view_name NOT LIKE 'ALL$%' AND view_name NOT LIKE 'ALL\\_%' ESCAPE '\\' AND view_name NOT LIKE 'DBA\\_%' ESCAPE '\\' AND view_name NOT LIKE 'USER\\_%' ESCAPE '\\' AND view_name NOT LIKE 'V$%' AND view_name NOT LIKE 'GV$%'"

        query = f"""
            SELECT 
                owner || '.' || table_name as name, 
                'TABLE' as type,
                num_rows
            FROM all_tables
            {table_filter}
            UNION ALL
            SELECT 
                owner || '.' || view_name as name, 
                'VIEW' as type,
                0 as num_rows
            FROM all_views
            {view_filter}
        """

        datasets = []
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                for row in cursor:
                    datasets.append(
                        {
                            "name": row[0],
                            "type": row[1],
                            "row_count": row[2] or 0,
                            "column_count": 0,  # Will be fetched per table
                            "last_refresh": datetime.now(timezone.utc).isoformat()
                            + "Z",
                        }
                    )
            self._cache["datasets"] = (datasets, now)
            return datasets

    def get_table_metadata(self, dataset_name: str) -> List[Dict[str, Any]]:
        """
        Fetch column metadata using ALL_TAB_COLUMNS.
        Supports schema-qualified names (e.g. 'MGBCM.REAL_DATA_1').
        """
        import time

        owner, table = self._parse_dataset_name(dataset_name)
        cache_key = f"metadata_{owner}.{table}"
        now = time.time()

        if cache_key in self._cache:
            cached_obj, cached_time = self._cache[cache_key]
            if now - cached_time < self._cache_ttl:
                return cached_obj

        query = """
            SELECT 
                column_name, 
                data_type, 
                nullable,
                data_precision,
                data_scale
            FROM all_tab_columns
            WHERE table_name = :name AND owner = :owner
            ORDER BY column_id
        """

        columns = []
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, {"name": table, "owner": owner})
                for row in cursor:
                    col_name = row[0]
                    col_type = row[1].upper()
                    is_nullable = row[2] == "Y"

                    # Determine base types for UI
                    is_numeric = any(
                        t in col_type for t in ["NUMBER", "FLOAT", "BINARY_DOUBLE"]
                    )
                    is_date = any(t in col_type for t in ["DATE", "TIMESTAMP"])
                    is_text = any(
                        t in col_type for t in ["VARCHAR2", "CHAR", "NVARCHAR2", "CLOB"]
                    )

                    columns.append(
                        {
                            "name": col_name,
                            "data_type": col_type,
                            "nullable": is_nullable,
                            "is_filterable": True,
                            "is_sortable": True,
                            "base_type": "numeric"
                            if is_numeric
                            else "date"
                            if is_date
                            else "text"
                            if is_text
                            else "other",
                        }
                    )

            self._cache[cache_key] = (columns, now)
            return columns

    def execute_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute query and return list of dictionaries."""
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or {})
                # Fetch column names
                columns = [col[0] for col in cursor.description]
                results = []
                for row in cursor:
                    results.append(dict(zip(columns, row)))
                return results

    def execute_query_df(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute and return as DataFrame."""
        with self.connection() as conn:
            return pd.read_sql(query, conn, params=params)

    def explain_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Executes EXPLAIN PLAN for the query and checks the estimated COST or CARDINALITY.
        Raises ValueError if the cost exceeds EXPLAIN_PLAN_THRESHOLD.
        """
        import uuid
        from app.core.config import get_settings

        settings = get_settings()
        stmt_id = f"EQ_{uuid.uuid4().hex[:8]}"

        explain_sql = f"EXPLAIN PLAN SET STATEMENT_ID = '{stmt_id}' FOR {query}"
        cost_query = (
            f"SELECT operation, options, object_name, cost, cardinality "
            f"FROM plan_table WHERE statement_id = '{stmt_id}' AND id = 0"
        )
        delete_query = f"DELETE FROM plan_table WHERE statement_id = '{stmt_id}'"

        with self.connection() as conn:
            with conn.cursor() as cursor:
                try:
                    # 1. Generate the explain plan
                    cursor.execute(explain_sql, params or {})

                    # 2. Read the top-level cost/cardinality (ID = 0 is the SELECT STATEMENT)
                    cursor.execute(cost_query)
                    row = cursor.fetchone()
                    if row:
                        cost = row[3] or 0
                        cardinality = row[4] or 0

                        max_allowed = settings.EXPLAIN_PLAN_THRESHOLD
                        # Oracle costs are abstract, but we flag if either metric exceeds our hard threshold
                        if cost > max_allowed or cardinality > max_allowed:
                            raise ValueError(
                                f"Query rejected: Estimated Cost ({cost}) or Cardinality ({cardinality}) "
                                f"exceeds the maximum allowed threshold of {max_allowed}. "
                                f"Please add more specific filters (e.g., date ranges) to narrow the dataset."
                            )
                finally:
                    # 3. Clean up the plan table safely
                    try:
                        cursor.execute(delete_query)
                        conn.commit()
                    except Exception as e:
                        logger.warning(f"Failed to clear EXPLAIN plan '{stmt_id}': {e}")

    def get_row_count(
        self,
        dataset_name: str,
        filters_sql: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> int:
        owner, table = self._parse_dataset_name(dataset_name)
        query = f"SELECT COUNT(*) FROM {self._qualified_table(owner, table)}"
        if filters_sql:
            query += f" WHERE {filters_sql}"

        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or {})
                return cursor.fetchone()[0]

    def get_partition_values(
        self,
        dataset_name: str,
        partition_column: str,
        load_type_column: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        Fetch distinct partition values for a dataset's load ID column.
        Supports schema-qualified dataset names.
        """
        owner, table = self._parse_dataset_name(dataset_name)
        qualified = self._qualified_table(owner, table)
        col_name = partition_column.upper()

        if load_type_column:
            lt_col = load_type_column.upper()
            query = f'SELECT DISTINCT "{lt_col}", "{col_name}" FROM {qualified} ORDER BY "{col_name}" DESC'
            query = f"SELECT * FROM ({query}) WHERE ROWNUM <= {limit}"

            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    values = []
                    seen_values = set()
                    values_map = {}
                    for row in cursor:
                        lt_val = str(row[0]) if row[0] is not None else "UNKNOWN"
                        id_val = row[1]

                        if id_val not in seen_values:
                            values.append(id_val)
                            seen_values.add(id_val)

                        if lt_val not in values_map:
                            values_map[lt_val] = []
                        values_map[lt_val].append(id_val)

                    # Ensure values_map also has unique lists
                    for k in values_map:
                        values_map[k] = sorted(list(set(values_map[k])), reverse=True)

                    return {
                        "values": values,
                        "values_map": values_map,
                        "max_value": values[0] if values else None,
                        "min_value": values[-1] if values else None,
                    }
        else:
            query = f'SELECT DISTINCT "{col_name}" FROM {qualified} ORDER BY "{col_name}" DESC'
            # Add Oracle row limit
            query = f"SELECT * FROM ({query}) WHERE ROWNUM <= {limit}"

            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    values = [row[0] for row in cursor]
                    return {
                        "values": values,
                        "max_value": values[0] if values else None,
                        "min_value": values[-1] if values else None,
                    }

    def execute_query_cursor(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_size: int = 10000,
    ):
        """
        Execute a parameterized SQL query and yield results in chunks.
        """
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or {})
                columns = [col[0] for col in cursor.description]
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    yield [dict(zip(columns, row)) for row in rows]

    def get_pool_metrics(self) -> Dict[str, int]:
        """Returns current utilization of the database pool."""
        return {
            "pool_max": self.pool.max,
            "pool_min": self.pool.min,
            "pool_busy": self.pool.busy,
            "pool_open": self.pool.opened,
            "pool_wait_count": self.pool.getwaitcount(),
        }

    def close(self):
        from app.core.logger import logger

        logger.info("Closing Oracle connection pool...")
        try:
            self.pool.close()
            logger.info("Oracle connection pool closed successfully.")
        except Exception as e:
            logger.error(f"Error closing Oracle connection pool: {e}")
