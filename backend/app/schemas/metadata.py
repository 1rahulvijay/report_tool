from pydantic import BaseModel, Field
from typing import Any, List, Optional, Dict
from datetime import datetime


class DatasetMetadata(BaseModel):
    """
    Metadata representation of a dynamically discovered dataset (table or view).
    """

    name: str = Field(..., description="Name of the table or view")
    display_name: Optional[str] = Field(
        None, description="User-friendly display name from table_config.json"
    )
    row_count: int = Field(..., description="Total number of rows in the dataset")
    column_count: int = Field(..., description="Total number of columns in the dataset")
    last_refresh: Optional[str] = Field(
        None, description="ISO-8601 timestamp of last refresh date, if available"
    )
    type: str = Field(..., description="Type of dataset, usually 'TABLE' or 'VIEW'")


class ColumnMetadata(BaseModel):
    """
    Schema representation for a single column inside a dataset.
    This drives the frontend filter UI (e.g., numeric vs date inputs).
    """

    name: str = Field(..., description="Name of the column")
    display_name: Optional[str] = Field(
        None, description="User-friendly display name from table_config.json"
    )
    data_type: str = Field(
        ..., description="Raw database data type (e.g. VARCHAR, INTEGER, DATE)"
    )
    nullable: bool = Field(
        ..., description="Whether the column can contain null values"
    )
    is_filterable: bool = Field(
        ..., description="Whether this column supports filtering operations"
    )
    is_sortable: bool = Field(
        ..., description="Whether this column supports sorting operations"
    )
    base_type: str = Field(
        ...,
        description="Simplified type for UI rendering: 'text', 'numeric', 'date', or 'other'",
    )


class PartitionInfo(BaseModel):
    """
    Metadata about a dataset's load/partition column.
    Returned alongside column metadata so the frontend can build a vintage selector.
    """

    load_type_column: Optional[str] = Field(
        None,
        description="Optional column indicating frequency (e.g. 'Daily', 'Monthly')",
    )
    load_id_column: Optional[str] = Field(
        None, description="The actual database column used for filtering the partition"
    )
    date_column: Optional[str] = Field(
        None, description="The user-friendly date column shown in the UI"
    )
    supported_types: List[str] = Field(
        default_factory=list,
        description="Array of supported frequencies (e.g. ['Monthly', 'Daily'])",
    )
    available_values: List[Any] = Field(
        default_factory=list,
        description="Distinct partition values in descending order (most recent first)",
    )
    available_values_map: Optional[Dict[str, List[Any]]] = Field(
        None,
        description="Map of load_type -> list of distinct partition values",
    )
    max_value: Optional[Any] = Field(
        None, description="Latest available partition value"
    )
    min_value: Optional[Any] = Field(
        None, description="Earliest available partition value"
    )


class DatasetColumnsResponse(BaseModel):
    """
    Response model for /datasets/{dataset}/columns API.
    """

    dataset_name: str
    columns: List[ColumnMetadata]
    partition_info: Optional[PartitionInfo] = Field(
        None, description="Partition metadata if the dataset has a load ID column"
    )


class DatasetListResponse(BaseModel):
    """
    Response model for /datasets API.
    """

    datasets: List[DatasetMetadata]
