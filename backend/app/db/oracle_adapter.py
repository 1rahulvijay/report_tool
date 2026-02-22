import oracledb
from typing import Any, Dict, List, Optional
import pandas as pd
from datetime import datetime
import contextlib

from .base import BaseDatabaseAdapter


class OracleAdapter(BaseDatabaseAdapter):
    """
    Enterprise Oracle implementation of the database adapter.
    Supports connection pooling and handles large metadata discovery.
    """

    def __init__(
        self, user: str, password: str, dsn: str, min_pool: int = 5, max_pool: int = 20
    ):
        self.pool = oracledb.create_pool(
            user=user,
            password=password,
            dsn=dsn,
            min=min_pool,
            max=max_pool,
            increment=1,
            wait_timeout=5000,
        )
        self._cache = {}
        self._cache_ttl = 3600  # 1 hour

    @contextlib.contextmanager
    def connection(self):
        """Safe connection context manager with auto-release and retry logic."""
        import time

        max_retries = 3
        retry_delay = 1.0

        for attempt in range(max_retries):
            try:
                # Add wait_timeout to fail fast if pool is exhausted
                conn = self.pool.acquire()  # 5 seconds wait
                try:
                    yield conn
                finally:
                    self.pool.release(conn)
                return
            except oracledb.DatabaseError as e:
                # ORA-24459 or similar pool exhaustion errors
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                raise ValueError(
                    "Database connection pool exhausted. Please try again in a moment."
                ) from e

    def get_datasets(self) -> List[Dict[str, Any]]:
        """
        Dynamically discover available tables and views in the current schema.
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
            table_filter = "WHERE owner = :owner"
            view_filter = "WHERE owner = :owner"
            params["owner"] = settings.ORACLE_SCHEMA_FILTER.upper()
        else:
            sys_owners = "('SYS', 'SYSTEM', 'XDB', 'CTXSYS', 'MDSYS', 'ORDSYS', 'OUTLN', 'WMSYS', 'APPQOSSYS', 'DBSNMP', 'OJVMSYS', 'DVSYS', 'LBACSYS', 'AUDSYS', 'GSMADMIN_INTERNAL', 'ORACLE_OCM', 'PUBLIC', 'SYSMAN', 'EXFSYS', 'DIP', 'ANONYMOUS', 'XS$NULL', 'OACSYS')"
            table_filter = f"WHERE owner NOT IN {sys_owners} AND table_name NOT LIKE 'ALL$%' AND table_name NOT LIKE 'ALL\\_%' ESCAPE '\\' AND table_name NOT LIKE 'DBA\\_%' ESCAPE '\\' AND table_name NOT LIKE 'USER\\_%' ESCAPE '\\'"
            view_filter = f"WHERE owner NOT IN {sys_owners} AND view_name NOT LIKE 'ALL$%' AND view_name NOT LIKE 'ALL\\_%' ESCAPE '\\' AND view_name NOT LIKE 'DBA\\_%' ESCAPE '\\' AND view_name NOT LIKE 'USER\\_%' ESCAPE '\\' AND view_name NOT LIKE 'V$%' AND view_name NOT LIKE 'GV$%'"

        query = f"""
            SELECT 
                table_name as name, 
                'TABLE' as type,
                num_rows
            FROM all_tables
            {table_filter}
            UNION ALL
            SELECT 
                view_name as name, 
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
                            "last_refresh": datetime.utcnow().isoformat() + "Z",
                        }
                    )
            self._cache["datasets"] = (datasets, now)
            return datasets

    def get_table_metadata(self, dataset_name: str) -> List[Dict[str, Any]]:
        """
        Fetch column metadata using ALL_TAB_COLUMNS.
        """
        import time

        cache_key = f"metadata_{dataset_name.upper()}"
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
            WHERE table_name = :name
            ORDER BY column_id
        """

        columns = []
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, {"name": dataset_name.upper()})
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

    def get_row_count(
        self,
        dataset_name: str,
        filters_sql: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> int:
        query = f'SELECT COUNT(*) FROM "{dataset_name}"'
        if filters_sql:
            query += f" WHERE {filters_sql}"

        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or {})
                return cursor.fetchone()[0]

    def explain_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        Runs an EXPLAIN plan against the query and returns the estimated maximum cardinality.
        Raises ValueError if the cost exceeds the threshold or if the explain fails entirely.
        """
        from app.core.config import get_settings

        settings = get_settings()

        try:
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    # Oracle explain plan
                    cursor.execute(f"EXPLAIN PLAN FOR {query}", params or {})
                    cursor.execute(
                        "SELECT cardinality FROM plan_table WHERE id = 0 AND statement_id IS NULL"
                    )
                    row = cursor.fetchone()
                    cost = int(row[0]) if row and row[0] is not None else 0

                    if cost > settings.EXPLAIN_PLAN_THRESHOLD:
                        raise ValueError(
                            f"Query cost ({cost:,} rows) exceeds maximum allowed threshold ({settings.EXPLAIN_PLAN_THRESHOLD:,} rows). Please add more filters."
                        )
                    return cost
        except ValueError:
            raise
        except Exception as e:
            # If explain fails completely, err on the side of caution
            raise ValueError(
                "Unable to determine query cost. Query rejected to protect database performance."
            ) from e

    def get_partition_values(
        self,
        dataset_name: str,
        partition_column: str,
        load_type_column: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        Fetch distinct partition values for a dataset's load ID column.
        """
        table_name = dataset_name.upper()
        col_name = partition_column.upper()

        if load_type_column:
            lt_col = load_type_column.upper()
            query = f'SELECT DISTINCT "{lt_col}", "{col_name}" FROM "{table_name}" ORDER BY "{col_name}" DESC'
            query = f"SELECT * FROM ({query}) WHERE ROWNUM <= {limit}"

            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    values = []
                    values_map = {}
                    for row in cursor:
                        lt_val = str(row[0]) if row[0] is not None else "UNKNOWN"
                        id_val = row[1]
                        values.append(id_val)
                        if lt_val not in values_map:
                            values_map[lt_val] = []
                        values_map[lt_val].append(id_val)

                    return {
                        "values": values,
                        "values_map": values_map,
                        "max_value": values[0] if values else None,
                        "min_value": values[-1] if values else None,
                    }
        else:
            query = f'SELECT DISTINCT "{col_name}" FROM "{table_name}" ORDER BY "{col_name}" DESC'
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

    def close(self):
        self.pool.close()
