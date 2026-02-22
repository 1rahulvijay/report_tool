from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import pandas as pd


class BaseDatabaseAdapter(ABC):
    """
    Abstract base class defining the contract for all database adapters.
    This ensures the reporting engine is completely decoupled from the
    underlying database technology (DuckDB, Oracle, etc).
    """

    @abstractmethod
    def get_datasets(self) -> List[Dict[str, Any]]:
        """
        Dynamically discover available datasets (tables/views).
        Should return a list of dictionaries containing dataset metadata:
        - name: str
        - row_count: int
        - column_count: int
        - last_refresh: datetime (optional)
        - type: str ('TABLE' or 'VIEW')
        """
        pass

    @abstractmethod
    def get_table_metadata(self, dataset_name: str) -> List[Dict[str, Any]]:
        """
        Dynamically inspect the schema of a specific dataset.
        Should return a list of dictionaries defining columns:
        - name: str
        - data_type: str
        - nullable: bool
        - is_filterable: bool
        - is_sortable: bool
        """
        pass

    @abstractmethod
    def explain_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        Runs an EXPLAIN plan against the query and returns the estimated maximum cardinality (row count).
        Raises an exception if the query cost is astronomically high.
        """
        pass

    @abstractmethod
    def execute_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a parameterized SQL query securely and return rows as dictionaries.
        """
        pass

    @abstractmethod
    def execute_query_df(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Execute a parameterized SQL query securely and return a pandas DataFrame.
        Useful for analytical workloads or streaming exports.
        """
        pass

    @abstractmethod
    def get_row_count(
        self,
        dataset_name: str,
        filters_sql: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Get the total row count for a dataset, optionally applying a WHERE clause.
        Used for pagination metadata.
        """
        pass

    @abstractmethod
    def get_partition_values(
        self,
        dataset_name: str,
        partition_column: str,
        limit: int = 24,
    ) -> Dict[str, Any]:
        """
        Fetch distinct partition values for a dataset's load ID column.
        Returns: {"values": [...], "max_value": ..., "min_value": ...}
        Values are ordered descending (most recent first), limited to `limit` entries.
        """
        pass

    @abstractmethod
    def execute_query_cursor(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_size: int = 10000,
    ):
        """
        Execute a parameterized SQL query and yield results in chunks.
        Each chunk is a list of dicts (up to chunk_size rows).
        Used for memory-efficient streaming exports of large datasets.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close the database connection cleanly.
        """
        pass
