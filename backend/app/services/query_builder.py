from typing import Tuple, Dict, Any
from app.core.partition_config import get_partition_config
from app.schemas.query import (
    QueryRequest,
    LogicalGroup,
    FilterCondition,
    FilterOperator,
)


class SQLGenerationError(Exception):
    """Raised when the query builder encounters an invalid or unsafe filter state."""

    pass


class QueryBuilderService:
    """
    Core engine responsible for safely translating recursive JSON filter schemas
    into parameterized SQL statements for backend execution.
    """

    def __init__(self):
        # Maps the Enum FilterOperators to actual SQL syntax
        self.operator_map = {
            FilterOperator.EQUALS: "=",
            FilterOperator.NOT_EQUALS: "!=",
            FilterOperator.GREATER_THAN: ">",
            FilterOperator.GREATER_THAN_EQUAL: ">=",
            FilterOperator.LESS_THAN: "<",
            FilterOperator.LESS_THAN_EQUAL: "<=",
            FilterOperator.IN: "IN",
            FilterOperator.NOT_IN: "NOT IN",
            FilterOperator.IS_NULL: "IS NULL",
            FilterOperator.IS_NOT_NULL: "IS NOT NULL",
            FilterOperator.BETWEEN: "BETWEEN",
        }

    def _get_placeholder(self, param_name: str, index: int) -> str:
        """Returns the parameter placeholder for Oracle."""
        # Oracle prefers :1, :2 or :name. We'll use :name for consistency with dict params.
        return f":{param_name}"

    def _quote_identifier(self, identifier: str) -> str:
        """
        Safely quote a table or column name.
        Supports qualified identifiers like "table.column" -> "table"."column".
        """

        def quote(s):
            # Escape double quotes by doubling them
            val = str(s).replace('"', '""')
            return f'"{val}"'

        if "." in identifier:
            parts = identifier.split(".")
            return ".".join(quote(p) for p in parts)

        return quote(identifier)

    def _sanitize_alias(self, alias: str, max_length: int = 50) -> str:
        """
        Sanitize a user-provided output alias to prevent SQL injection (TC-INP-02)
        and enforce identifier length limits (TC-INP-01).

        Rules:
          1. Replace any non-alphanumeric character (except underscore) with '_'
          2. Truncate to max_length characters
          3. Strip leading/trailing underscores
          4. If result is empty, return a fallback
        """
        import re

        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", str(alias))
        sanitized = sanitized.strip("_")
        sanitized = sanitized[:max_length]
        return sanitized if sanitized else "unnamed_metric"

    def _parse_condition(
        self,
        condition: FilterCondition,
        param_counter: int,
        column_metadata: Dict[str, Any] = None,
    ) -> Tuple[str, Dict[str, Any], int]:
        """
        Parse a single FilterCondition into a SQL snippet and parameter dict.
        Returns: (sql_snippet, param_dict, updated_param_counter)
        """
        column_ident = self._quote_identifier(condition.column)
        op = condition.operator
        val = condition.value

        # Normalize operator to string for robust matching
        op_str = str(op.value if hasattr(op, "value") else op).lower()

        sql = ""
        params = {}

        # Handle Nulls & Emptiness
        if op_str == "is_null":
            return f"{column_ident} IS NULL", params, param_counter
        if op_str == "is_not_null":
            return f"{column_ident} IS NOT NULL", params, param_counter

        # TC-API-02: Type-aware SQL generation for emptiness
        # Only use blank string check if the column is a known text type
        is_text_type = False
        dt = getattr(condition, "datatype", "string")
        if dt == "string":
            is_text_type = True
        elif column_metadata:
            meta = column_metadata.get(condition.column)
            if meta:
                # fallback if datatype not explicitly provided
                b_type = meta.get("base_type") if isinstance(meta, dict) else str(meta)
                if b_type and b_type.lower() in ("text", "string", "varchar"):
                    is_text_type = True

        if op_str == "is_empty":
            if is_text_type:
                return (
                    f"({column_ident} IS NULL OR {column_ident} = '')",
                    params,
                    param_counter,
                )
            else:
                return f"{column_ident} IS NULL", params, param_counter

        if op_str == "is_not_empty":
            if is_text_type:
                return (
                    f"({column_ident} IS NOT NULL AND {column_ident} != '')",
                    params,
                    param_counter,
                )
            else:
                return f"{column_ident} IS NOT NULL", params, param_counter

        # Fix Falsy Zero Bug: Explicitly check against None and empty strings.
        # This treats '0' and 0 as valid values, safely bypassing empty input gracefully.
        if val is None or val == "":
            return "1=1", params, param_counter

        # Handle text specific wildcards and case-insensitive equality
        if (
            op_str
            in ["contains", "not_contains", "starts_with", "ends_with", "eq", "neq"]
            and is_text_type
        ):
            p_name = f"p_{param_counter}"
            placeholder = self._get_placeholder(p_name, param_counter)

            sql_op = "NOT LIKE" if op_str in ["not_contains", "neq"] else "LIKE"
            sql = f"UPPER(CAST({column_ident} AS VARCHAR2(4000))) {sql_op} UPPER({placeholder})"

            # Ensure value is treated as string for wildcards
            val_str = str(val)
            if op_str in ["contains", "not_contains"]:
                params[p_name] = f"%{val_str}%"
            elif op_str == "starts_with":
                params[p_name] = f"{val_str}%"
            elif op_str == "ends_with":
                params[p_name] = f"%{val_str}"
            elif op_str in ["eq", "neq"]:
                params[p_name] = f"{val_str}"

            return sql, params, param_counter + 1

        # Handle IN / NOT IN (Arrays)
        if op_str in ["in", "not_in"]:
            # Normalize value to a list
            if isinstance(val, str):
                items = [item.strip() for item in val.split(",") if item.strip()]
                val = items
            elif not isinstance(val, list):
                # Handle numeric or other scalar values
                val = [val]

            if not val:
                # Return empty match instead of error to keep UI fluid during typing
                return "1=0" if op_str == "in" else "1=1", {}, param_counter

            sql_op = "IN" if op_str == "in" else "NOT IN"
            placeholders = []

            for item in val:
                p_name = f"p_{param_counter}"
                placeholder = self._get_placeholder(p_name, param_counter)
                placeholders.append(placeholder)
                params[p_name] = item
                param_counter += 1

            sql = f"{column_ident} {sql_op} ({', '.join(placeholders)})"
            return sql, params, param_counter

        # Handle BETWEEN (Arrays of exactly 2)
        if op_str == "between":
            if not isinstance(val, list) or len(val) != 2:
                raise SQLGenerationError(
                    f"Value must be a list of EXACTLY 2 items for operator '{op_str}'"
                )

            p_start = f"p_{param_counter}"
            p_end = f"p_{param_counter + 1}"
            placeholder_start = self._get_placeholder(p_start, param_counter)
            placeholder_end = self._get_placeholder(p_end, param_counter + 1)

            # Safe handling for dates/numeric in BETWEEN
            sql = f"{column_ident} BETWEEN {placeholder_start} AND {placeholder_end}"
            params[p_start] = val[0]
            params[p_end] = val[1]

            return sql, params, param_counter + 2

        # Handle standard operators (=, !=, >, <, >=, <=)
        # Reverse map string back to Enum if possible for operator_map lookup
        standard_ops = {
            "eq": "=",
            "neq": "!=",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
        }
        if op_str in standard_ops:
            sql_op = standard_ops[op_str]
            p_name = f"p_{param_counter}"
            placeholder = self._get_placeholder(p_name, param_counter)

            sql = f"{column_ident} {sql_op} {placeholder}"
            params[p_name] = val

            return sql, params, param_counter + 1

        raise SQLGenerationError(f"Unsupported operator: {op_str}")

    def _parse_logical_group(
        self,
        group: LogicalGroup,
        param_counter: int,
        agg_aliases: set = None,
        column_metadata: Dict[str, Any] = None,
    ) -> Tuple[str, str, Dict[str, Any], int]:
        """
        Recursively parses a LogicalGroup (AND/OR tree) into safe parameterized SQL WHERE and HAVING strings.
        Routes filters on aggregation columns to HAVING instead of WHERE.
        """
        if agg_aliases is None:
            agg_aliases = set()

        if not group.conditions:
            return "", "", {}, param_counter

        where_snippets = []
        having_snippets = []
        all_params = {}

        for item in group.conditions:
            if isinstance(item, FilterCondition):
                sql, params, param_counter = self._parse_condition(
                    item, param_counter, column_metadata
                )

                # Check if this column targets an aggregation alias
                if item.column in agg_aliases:
                    having_snippets.append(f"({sql})")
                else:
                    where_snippets.append(f"({sql})")

            elif isinstance(item, dict) and "column" in item:
                condition = FilterCondition(**item)
                sql, params, param_counter = self._parse_condition(
                    condition, param_counter, column_metadata
                )

                if condition.column in agg_aliases:
                    having_snippets.append(f"({sql})")
                else:
                    where_snippets.append(f"({sql})")

            elif isinstance(item, LogicalGroup):
                w_sql, h_sql, params, param_counter = self._parse_logical_group(
                    item, param_counter, agg_aliases, column_metadata
                )
                if w_sql and h_sql and group.logic == "OR":
                    raise SQLGenerationError(
                        "Cannot mix raw and aggregated filters in an OR logical group currently."
                    )

                if w_sql:
                    where_snippets.append(f"({w_sql})")
                if h_sql:
                    having_snippets.append(f"({h_sql})")

            elif isinstance(item, dict) and "logic" in item:
                # Handle raw dict recursion
                nested_group = LogicalGroup(**item)
                w_sql, h_sql, params, param_counter = self._parse_logical_group(
                    nested_group, param_counter, agg_aliases, column_metadata
                )
                if w_sql and h_sql and group.logic == "OR":
                    raise SQLGenerationError(
                        "Cannot mix raw and aggregated filters in an OR logical group currently."
                    )

                if w_sql:
                    where_snippets.append(f"({w_sql})")
                if h_sql:
                    having_snippets.append(f"({h_sql})")
            else:
                raise SQLGenerationError(f"Invalid item in logical group: {type(item)}")

            all_params.update(params)

        where_final = ""
        having_final = ""

        logic_operator = f" {group.logic} "
        if where_snippets:
            where_final = logic_operator.join(where_snippets)
        if having_snippets:
            having_final = logic_operator.join(having_snippets)

        return where_final, having_final, all_params, param_counter

    def _split_filters_for_dataset(
        self, item: Any, target_dataset: str, base_dataset: str, agg_aliases: set
    ) -> Tuple[Any, Any]:
        """
        Splits a LogicalGroup AST to extract the conditions strictly applying to a single dataset.
        Returns a tuple of (pushed_down_group, remaining_group).
        """
        from app.schemas.query import FilterCondition, LogicalGroup

        if isinstance(item, dict):
            if "column" in item:
                item = FilterCondition(**item)
            elif "logic" in item:
                item = LogicalGroup(**item)

        if isinstance(item, FilterCondition):
            # Do not push down post-aggregation filters
            if item.column in agg_aliases:
                return None, item
            prefix = f"{target_dataset}."
            if item.column.startswith(prefix) or (
                "." not in item.column and target_dataset == base_dataset
            ):
                return item, None
            return None, item

        if isinstance(item, LogicalGroup):
            if item.logic == "OR":
                # For OR, ALL must be pushable.
                all_pushable = True
                for cond in item.conditions:
                    p, r = self._split_filters_for_dataset(
                        cond, target_dataset, base_dataset, agg_aliases
                    )
                    if r is not None:
                        all_pushable = False
                        break
                if all_pushable and item.conditions:
                    return item, None
                return None, item

            elif item.logic == "AND":
                pushed = []
                remaining = []
                for cond in item.conditions:
                    p, r = self._split_filters_for_dataset(
                        cond, target_dataset, base_dataset, agg_aliases
                    )
                    if p is not None:
                        pushed.append(p)
                    if r is not None:
                        remaining.append(r)

                p_group = (
                    LogicalGroup(logic="AND", conditions=pushed) if pushed else None
                )
                r_group = (
                    LogicalGroup(logic="AND", conditions=remaining)
                    if remaining
                    else None
                )
                return p_group, r_group

        return None, item

    def build_query(self, request: QueryRequest) -> Tuple[str, Dict[str, Any]]:
        """
        Main entry point to assemble a full SECURE SELECT statement based on QueryRequest.
        Returns: (Full SQL Statement, Dict of bind parameters)
        """
        # 1. Alias Tracking (TC-JOIN-04: Self-Join Support)
        # Track dataset instances to provide unique table aliases
        dataset_occurrences = {}

        def get_unique_alias(ds_name: str) -> str:
            count = dataset_occurrences.get(ds_name, 0)
            dataset_occurrences[ds_name] = count + 1
            return ds_name if count == 0 else f"{ds_name}_{count}"

        base_alias = get_unique_alias(request.dataset)
        alias_map = {request.dataset: base_alias}

        join_aliases = []
        if request.joins:
            for join in request.joins:
                alias = get_unique_alias(join.right_dataset)
                join_aliases.append(alias)
                alias_map[join.right_dataset] = alias

        # 1. SELECT Clause
        hint = ""
        if request.use_high_perf_hints:
            hint = "/*+ INMEMORY */ "

        # Handle Aggregations vs Standard columns
        if request.aggregations and len(request.aggregations) > 0:
            select_parts = []
            if request.group_by:
                for gb_col in request.group_by:
                    res_col = gb_col
                    if "." in gb_col:
                        ds, cname = gb_col.split(".", 1)
                        if ds in alias_map:
                            res_col = f"{alias_map[ds]}.{cname}"

                    col_ident = self._quote_identifier(res_col)
                    select_parts.append(f'{col_ident} AS "{gb_col}"')

            func_map = {
                "sum": "SUM",
                "avg": "AVG",
                "count": "COUNT",
                "max": "MAX",
                "min": "MIN",
                "distinct_count": "COUNT(DISTINCT",
            }

            used_output_names = set()
            for agg in request.aggregations:
                func = func_map.get(agg.function.lower(), "SUM")

                res_agg_col = agg.column
                if "." in agg.column:
                    ds, cname = agg.column.split(".", 1)
                    if ds in alias_map:
                        res_agg_col = f"{alias_map[ds]}.{cname}"

                col = self._quote_identifier(res_agg_col)

                # TC-AGG-07: Unique auto-aliasing
                # TC-INP-02: Sanitize user-supplied alias to prevent SQL injection
                raw_output = (
                    agg.output_name.strip()
                    if agg.output_name and agg.output_name.strip()
                    else f"{agg.function.upper()}_{agg.column}"
                )
                base_output = self._sanitize_alias(raw_output)

                final_output = base_output
                suffix = 1
                while final_output in used_output_names:
                    final_output = f"{base_output}_{suffix}"
                    suffix += 1

                used_output_names.add(final_output)
                output = self._quote_identifier(final_output)

                if agg.function.lower() == "distinct_count":
                    select_parts.append(f"{func} {col}) AS {output}")
                else:
                    select_parts.append(f"{func}({col}) AS {output}")

            select_clause = f"{hint}" + ", ".join(select_parts)

        elif request.columns and len(request.columns) > 0:
            quoted_cols = []
            for c in request.columns:
                full_name = f"{request.dataset}.{c}" if "." not in c else c

                res_full_name = full_name
                if "." in full_name:
                    ds, cname = full_name.split(".", 1)
                    if ds in alias_map:
                        res_full_name = f"{alias_map[ds]}.{cname}"

                quoted = self._quote_identifier(res_full_name)
                quoted_cols.append(f'{quoted} AS "{full_name}"')
            select_clause = f"{hint}" + ", ".join(quoted_cols)
        else:
            raise SQLGenerationError(
                "Aggressive Column Pruning strictly prohibits 'SELECT *' unbound memory payloads."
            )

        # Collect Aggregation Aliases before pushdown to avoid filtering post-aggregations inline
        agg_aliases = set()
        if request.aggregations:
            for agg in request.aggregations:
                if agg.output_name and agg.output_name.strip():
                    agg_aliases.add(agg.output_name)
                else:
                    agg_aliases.add(f"{agg.function.upper()}_{agg.column}")

        # Compute Predicate Pushdown Map
        all_datasets = [request.dataset]
        if request.joins:
            all_datasets.extend([j.right_dataset for j in request.joins])

        remaining_filters = request.filters
        pushdown_map = {}
        if remaining_filters and getattr(remaining_filters, "conditions", None):
            for ds in all_datasets:
                pushed, remaining = self._split_filters_for_dataset(
                    remaining_filters, ds, request.dataset, agg_aliases
                )
                if pushed and getattr(pushed, "conditions", None):
                    pushdown_map[ds] = pushed
                remaining_filters = remaining
                if not remaining_filters:
                    break

        param_counter = 1
        params = {}

        def resolve_dataset_source(ds_name: str, alias: str) -> str:
            """Resolves a physical table into a pre-filtered Subquery/CTE.
            Applies partition predicate FIRST, then user pushdown filters."""
            nonlocal param_counter
            ident = self._quote_identifier(ds_name)
            alias_ident = self._quote_identifier(alias)

            where_parts = []

            # 1. Partition predicate injection (TC-PART-01 through TC-PART-04)
            if request.partition_filters and ds_name in request.partition_filters:
                part_cfg = get_partition_config(ds_name)
                if part_cfg:
                    part_col = self._quote_identifier(part_cfg["column"])
                    part_values = request.partition_filters[ds_name]
                    if len(part_values) == 1:
                        p_name = f"part_{ds_name}_{param_counter}"
                        placeholder = self._get_placeholder(p_name, param_counter)
                        where_parts.append(f"{part_col} = {placeholder}")
                        params[p_name] = part_values[0]
                        param_counter += 1
                    elif len(part_values) > 1:
                        in_placeholders = []
                        for v in part_values:
                            p_name = f"part_{ds_name}_{param_counter}"
                            placeholder = self._get_placeholder(p_name, param_counter)
                            in_placeholders.append(placeholder)
                            params[p_name] = v
                            param_counter += 1
                        where_parts.append(
                            f"{part_col} IN ({', '.join(in_placeholders)})"
                        )

            # 2. User filter pushdown (existing logic)
            ds_filters = pushdown_map.get(ds_name)
            if ds_filters:
                w_sql, _, p_params, param_counter = self._parse_logical_group(
                    ds_filters, param_counter, agg_aliases, request.column_metadata
                )
                params.update(p_params)
                if w_sql:
                    where_parts.append(w_sql)

            # 3. Combine into subquery if any predicates exist
            if where_parts:
                combined = " AND ".join(where_parts)
                return f"(SELECT * FROM {ident} WHERE {combined}) {alias_ident}"
            return f"{ident} {alias_ident}"

        # Smart Join Ordering Heuristic
        # Order the primary joins so heavily filtered datasets are evaluated immediately
        if request.joins:
            base_joins = [j for j in request.joins if j.left_dataset == request.dataset]
            dependent_joins = [
                j for j in request.joins if j.left_dataset != request.dataset
            ]

            def filter_weight(j):
                group = pushdown_map.get(j.right_dataset)
                return len(getattr(group, "conditions", [])) if group else 0

            base_joins = sorted(base_joins, key=filter_weight, reverse=True)
            request.joins = base_joins + dependent_joins

        # 2. FROM Clause (Using inline CTEs for heavily filtered tables)
        base_source = resolve_dataset_source(request.dataset, base_alias)
        from_clause = f"FROM {base_source}"

        if request.joins:
            for i, join in enumerate(request.joins):
                # Map Join types to valid SQL keywords
                join_type_map = {
                    "inner": "INNER",
                    "left": "LEFT",
                    "right": "RIGHT",
                    "outer": "FULL OUTER",
                }
                join_type = join_type_map.get(join.join_type.lower(), "INNER")
                right_alias = join_aliases[i]
                right_source = resolve_dataset_source(join.right_dataset, right_alias)

                on_clauses = []
                for cond in join.on:
                    # Resolve aliases for ON clause
                    l_ds = join.left_dataset
                    l_alias = alias_map.get(l_ds, l_ds)

                    left_full = (
                        f"{l_alias}.{cond.left_column}"
                        if "." not in cond.left_column
                        else cond.left_column
                    )
                    right_full = (
                        f"{right_alias}.{cond.right_column}"
                        if "." not in cond.right_column
                        else cond.right_column
                    )

                    left_col = self._quote_identifier(left_full)
                    right_col = self._quote_identifier(right_full)
                    on_clauses.append(f"{left_col} = {right_col}")

                from_clause += (
                    f"\n{join_type} JOIN {right_source} ON {' AND '.join(on_clauses)}"
                )

        sql = f"SELECT {select_clause}\n{from_clause}"

        # 3. Global WHERE/HAVING Clause (Remaining unresolved complex logic trees)
        having_sql = ""
        where_sql = ""
        if remaining_filters and getattr(remaining_filters, "conditions", None):
            w_sql, h_sql, f_params, param_counter = self._parse_logical_group(
                remaining_filters, param_counter, agg_aliases, request.column_metadata
            )
            params.update(f_params)
            where_sql = w_sql
            having_sql = h_sql

        if where_sql:
            sql += f"\nWHERE {where_sql}"

        # 4. GROUP BY Clause
        if request.group_by and len(request.group_by) > 0:
            quoted_gb = [self._quote_identifier(c) for c in request.group_by]
            sql += f"\nGROUP BY {', '.join(quoted_gb)}"
            if having_sql:
                sql += f"\nHAVING {having_sql}"

        # 5. ORDER BY Clause
        if request.sorting and len(request.sorting) > 0:
            sort_snippets = []
            for sort in request.sorting:
                col_ident = self._quote_identifier(sort.column)
                dir_sql = "DESC" if sort.direction == "DESC" else "ASC"
                sort_snippets.append(f"{col_ident} {dir_sql}")
            sql += f"\nORDER BY {', '.join(sort_snippets)}"

        # 6. LIMIT / OFFSET
        sql += f"\nOFFSET {request.offset} ROWS FETCH NEXT {request.limit} ROWS ONLY"

        return sql, params

    def build_count_query(self, request: QueryRequest) -> Tuple[str, Dict[str, Any]]:
        """
        Builds a query specifically to fetch the total filtered row count.
        For grouped queries, returns the number of unique groups.
        """
        if request.group_by and len(request.group_by) > 0:
            # For Grouped results, the count is the count of unique groups.
            # Easiest way is a subquery.
            from app.core.config import get_settings

            inner_request = request.model_copy()
            inner_request.limit = get_settings().MAX_ROW_LIMIT
            inner_request.offset = 0
            inner_sql, params = self.build_query(inner_request)

            # Simple subquery count
            sql = f"SELECT COUNT(*) as total_rows FROM (\n{inner_sql}\n) sub"
            return sql, params

        dataset_ident = self._quote_identifier(request.dataset)
        from_clause = f"FROM {dataset_ident}"

        # Add JOINS
        if request.joins:
            for join in request.joins:
                # Map Join types to valid SQL keywords
                join_type_map = {
                    "inner": "INNER",
                    "left": "LEFT",
                    "right": "RIGHT",
                    "outer": "FULL OUTER",
                }
                join_type = join_type_map.get(join.join_type.lower(), "INNER")
                right_table = self._quote_identifier(join.right_dataset)
                on_clauses = []
                for cond in join.on:
                    # Robust qualification check: only prepend if not already qualified
                    left_full = (
                        f"{join.left_dataset}.{cond.left_column}"
                        if "." not in cond.left_column
                        else cond.left_column
                    )
                    right_full = (
                        f"{join.right_dataset}.{cond.right_column}"
                        if "." not in cond.right_column
                        else cond.right_column
                    )

                    left_col = self._quote_identifier(left_full)
                    right_col = self._quote_identifier(right_full)
                    on_clauses.append(f"{left_col} = {right_col}")

                from_clause += (
                    f"\n{join_type} JOIN {right_table} ON {' AND '.join(on_clauses)}"
                )

        sql = f"SELECT COUNT(*) as total_rows\n{from_clause}"
        params = {}

        # Collect defined aggregation aliases
        agg_aliases = set()
        if request.aggregations:
            for agg in request.aggregations:
                if agg.output_name and agg.output_name.strip():
                    agg_aliases.add(agg.output_name)
                else:
                    agg_aliases.add(f"{agg.function.upper()}_{agg.column}")

        having_sql = ""
        if request.filters and request.filters.conditions:
            where_sql, having_sql, filter_params, _ = self._parse_logical_group(
                request.filters,
                param_counter=1,
                agg_aliases=agg_aliases,
                column_metadata=request.column_metadata,
            )
            params.update(filter_params)

            if where_sql:
                sql += f"\nWHERE {where_sql}"

        # When building a count query without grouping, HAVING shouldn't realistically exist.
        # But if it does, it applies to the count wrapper.
        if having_sql and not (request.group_by and len(request.group_by) > 0):
            sql += f"\nHAVING {having_sql}"

        return sql, params
