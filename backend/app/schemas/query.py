from typing import List, Optional, Any, Union, Literal, Dict
from enum import Enum
from pydantic import BaseModel, Field, model_validator


class FilterOperator(str, Enum):
    """
    Supported filter operators that define how the query builder will
    construct the WHERE clauses.
    """

    # Text operators
    EQUALS = "eq"
    NOT_EQUALS = "neq"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"

    # Numeric/Date operators
    GREATER_THAN = "gt"
    GREATER_THAN_EQUAL = "gte"
    LESS_THAN = "lt"
    LESS_THAN_EQUAL = "lte"
    BETWEEN = "between"

    # Array operators
    IN = "in"
    NOT_IN = "not_in"

    # Null/Empty operators
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    IS_EMPTY = "is_empty"
    IS_NOT_EMPTY = "is_not_empty"


class FilterCondition(BaseModel):
    """
    A single filter condition targeting a specific column.
    """

    column: str = Field(..., description="The name of the column to filter on")
    datatype: Literal["number", "string", "date", "timestamp"] = Field(
        "string", description="The data type of the column being filtered"
    )
    operator: FilterOperator = Field(..., description="The operation to apply")
    # TC-API-01: Explicitly optional to prevent 422s on unary operators
    value: Optional[Any] = Field(
        None,
        description="The value to filter against. For 'between', expect a list of 2 items. Can be null for 'is_null'.",
    )

    @model_validator(mode="after")
    def validate_condition(self) -> "FilterCondition":
        op = self.operator
        dt = self.datatype
        val = self.value

        ALLOWED_OPERATORS = {
            "number": {
                FilterOperator.EQUALS,
                FilterOperator.NOT_EQUALS,
                FilterOperator.LESS_THAN,
                FilterOperator.GREATER_THAN,
                FilterOperator.LESS_THAN_EQUAL,
                FilterOperator.GREATER_THAN_EQUAL,
                FilterOperator.IN,
                FilterOperator.IS_NULL,
                FilterOperator.IS_NOT_NULL,
            },
            "string": {
                FilterOperator.EQUALS,
                FilterOperator.NOT_EQUALS,
                FilterOperator.CONTAINS,
                FilterOperator.NOT_CONTAINS,
                FilterOperator.STARTS_WITH,
                FilterOperator.ENDS_WITH,
                FilterOperator.IN,
                FilterOperator.IS_NULL,
                FilterOperator.IS_NOT_NULL,
            },
            "date": {
                FilterOperator.EQUALS,
                FilterOperator.NOT_EQUALS,
                FilterOperator.LESS_THAN,
                FilterOperator.GREATER_THAN,
                FilterOperator.LESS_THAN_EQUAL,
                FilterOperator.GREATER_THAN_EQUAL,
                FilterOperator.BETWEEN,
                FilterOperator.IS_NULL,
                FilterOperator.IS_NOT_NULL,
            },
            "timestamp": {
                FilterOperator.EQUALS,
                FilterOperator.NOT_EQUALS,
                FilterOperator.LESS_THAN,
                FilterOperator.GREATER_THAN,
                FilterOperator.LESS_THAN_EQUAL,
                FilterOperator.GREATER_THAN_EQUAL,
                FilterOperator.BETWEEN,
                FilterOperator.IS_NULL,
                FilterOperator.IS_NOT_NULL,
            },
        }

        if op not in ALLOWED_OPERATORS.get(dt, set()):
            raise ValueError(
                f"Operator '{op.value}' is not allowed for datatype '{dt}'"
            )

        if op in [FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL]:
            return self

        if op == FilterOperator.IN:
            if not isinstance(val, list):
                raise ValueError(f"Value must be a list for operator '{op.value}'")

        if op == FilterOperator.BETWEEN:
            if not isinstance(val, list) or len(val) != 2:
                raise ValueError(
                    f"Value must be a list of EXACTLY 2 items for operator '{op.value}'"
                )

        return self


class LogicalGroup(BaseModel):
    """
    A recursive representation of nested AND/OR logic.
    A group can contain conditions OR other nested groups.
    """

    logic: Literal["AND", "OR"] = Field(
        "AND", description="Logical operator mapping inner conditions"
    )
    conditions: List[Union[FilterCondition, "LogicalGroup"]] = Field(
        default_factory=list,
        description="A list of conditions or inner logical groups to apply recursively.",
    )


class SortCondition(BaseModel):
    """
    Defines sorting order for a specific column.
    """

    column: str = Field(..., description="The column to sort by")
    direction: Literal["ASC", "DESC"] = Field("ASC", description="Sort direction")


class JoinType(str, Enum):
    """Supported join types."""

    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"


class JoinOn(BaseModel):
    """A pair of columns to join on."""

    left_column: str = Field(..., description="Column from the left table/dataset")
    right_column: str = Field(..., description="Column from the right table/dataset")


class JoinCondition(BaseModel):
    """A single join operation definition."""

    left_dataset: str = Field(
        ...,
        description="The dataset to join FROM (usually the base or a previous join)",
    )
    right_dataset: str = Field(..., description="The dataset to join TO")
    join_type: JoinType = Field(
        JoinType.INNER, description="The type of join to perform"
    )
    on: List[JoinOn] = Field(..., description="One or more column pairs to join on")


class AggregationFunction(str, Enum):
    """Supported aggregation functions."""

    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MAX = "max"
    MIN = "min"
    DISTINCT_COUNT = "distinct_count"


class AggregationCondition(BaseModel):
    """Defines a single aggregation (e.g., Sum of Sales as Total_Sales)."""

    column: str = Field(..., description="The source column to aggregate")
    function: AggregationFunction = Field(..., description="The aggregation function")
    output_name: str = Field(..., description="The alias/name for the resulting metric")


class QueryRequest(BaseModel):
    """
    The main payload sent to /query/preview or /query/export.
    Defines exactly what the reporting engine needs to build an ad-hoc query.
    """

    dataset: str = Field(..., description="The name of the target dataset/table")
    columns: Optional[List[str]] = Field(
        None, description="List of columns to SELECT. If null, fetch all."
    )
    joins: Optional[List[JoinCondition]] = Field(
        None, description="Optional list of joins to perform"
    )
    filters: Optional[LogicalGroup] = Field(
        None, description="Optional nested AND/OR filter tree"
    )
    group_by: Optional[List[str]] = Field(
        None, description="Optional list of columns to group by"
    )
    aggregations: Optional[List[AggregationCondition]] = Field(
        None, description="Optional list of aggregations to perform"
    )
    sorting: Optional[List[SortCondition]] = Field(
        None, description="Optional ordered list of sorting conditions"
    )
    # TC-API-02: Support for type-aware SQL generation
    column_metadata: Optional[Dict[str, Any]] = Field(
        None, description="Optional map of column names to their types/metadata"
    )
    # Partition / Load ID filters — auto-injected by frontend, maps dataset→values
    partition_filters: Optional[Dict[str, List[Any]]] = Field(
        None,
        description="Maps dataset_name -> list of partition values to restrict queries to. "
        "E.g. {'employee_roster': [202602], 'department_budgets': [202601, 202602]}",
    )
    partition_load_type: Optional[str] = Field(
        None,
        description="The currently selected load type (e.g. 'Daily', 'Monthly'). "
        "Used to inject an additional WHERE predicate on the load_type_column.",
    )
    limit: int = Field(
        100,
        ge=1,
        le=100000,
        description="Max rows to return for preview (preventing UI crash)",
    )
    offset: int = Field(0, ge=0, description="Pagination offset")
    use_high_perf_hints: bool = Field(
        False, description="If true, inject Oracle In-Memory hints (/*+ INMEMORY */)"
    )
    is_virtual_scroll: bool = Field(
        False, description="Hint for the backend if we are in virtual scroll mode"
    )
    is_preview: bool = Field(
        False,
        description="If true, bypasses column pruning for initial counts/previews",
    )


class PreviewResponse(BaseModel):
    """
    The shape of the response returned to the frontend after querying.
    """

    dataset_name: str
    total_row_count: int = Field(
        ..., description="Row count after filters but before limit/offset pagination"
    )
    execution_time_ms: float = Field(
        ..., description="Time taken to execute the query in milliseconds"
    )
    data: List[dict] = Field(
        ..., description="The paginated rows returned by the query"
    )
    columns: List[str] = Field(
        ..., description="The actual columns returned in the SELECT statement"
    )


# Required for recursive pydantic models in Python < 3.10
LogicalGroup.update_forward_refs()
