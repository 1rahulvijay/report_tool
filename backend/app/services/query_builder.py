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

    def __init__(self, message: str, context: Any = None):
        if context:
            super().__init__(f"{message} (Context: {context})")
        else:
            super().__init__(message)
        self.context = context


class ParamGenerator:
    """Encapsulates parameter naming and value mapping to prevent collisions."""

    def __init__(self, start_counter: int = 1):
        self.counter = start_counter
        self.params: Dict[str, Any] = {}

    def get_next(self, prefix: str = "p") -> Tuple[str, str]:
        """Generates a new unique parameter name and placeholder."""
        name = f"{prefix}_{self.counter}"
        placeholder = f":{name}"
        self.counter += 1
        return name, placeholder

    def add(self, prefix: str, value: Any) -> Tuple[str, str]:
        """Helper to get a new parameter and immediately map its value."""
        name, placeholder = self.get_next(prefix)
        self.params[name] = value
        return name, placeholder

    def update(self, other_params: Dict[str, Any]):
        self.params.update(other_params)


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
        """
        import re

        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", str(alias))
        sanitized = sanitized.strip("_")
        sanitized = sanitized[:max_length]
        return sanitized if sanitized else "unnamed_metric"

    def _is_text_type(
        self, condition: FilterCondition, column_metadata: Dict[str, Any] = None
    ) -> bool:
        dt = getattr(condition, "datatype", "string")
        if dt == "string":
            return True
        elif column_metadata:
            meta = column_metadata.get(condition.column)
            if meta:
                b_type = meta.get("base_type") if isinstance(meta, dict) else str(meta)
                if b_type and b_type.lower() in ("text", "string", "varchar"):
                    return True
        return False

    def _handle_nullness(
        self,
        op_str: str,
        condition: FilterCondition,
        column_ident: str,
        column_metadata: Dict[str, Any],
    ) -> str:
        if op_str == "is_null":
            return f"{column_ident} IS NULL"
        if op_str == "is_not_null":
            return f"{column_ident} IS NOT NULL"

        is_text_type = self._is_text_type(condition, column_metadata)
        if op_str == "is_empty":
            if is_text_type:
                return f"({column_ident} IS NULL OR {column_ident} = '')"
            else:
                return f"{column_ident} IS NULL"

        if op_str == "is_not_empty":
            if is_text_type:
                return f"({column_ident} IS NOT NULL AND {column_ident} != '')"
            else:
                return f"{column_ident} IS NOT NULL"
        raise SQLGenerationError(
            f"Unknown nullness operator: {op_str}", context=condition
        )

    def _handle_text_wildcards(
        self, op_str: str, val: Any, column_ident: str, param_gen: ParamGenerator
    ) -> str:
        sql_op = "NOT LIKE" if op_str in ["not_contains", "neq"] else "LIKE"
        val_str = str(val)

        if op_str in ["contains", "not_contains"]:
            param_val = f"%{val_str}%"
        elif op_str == "starts_with":
            param_val = f"{val_str}%"
        elif op_str == "ends_with":
            param_val = f"%{val_str}"
        elif op_str in ["eq", "neq"]:
            param_val = f"{val_str}"

        _, placeholder = param_gen.add("p", param_val)
        return f"UPPER(CAST({column_ident} AS VARCHAR2(4000))) {sql_op} UPPER({placeholder})"

    def _handle_in_arrays(
        self, op_str: str, val: Any, column_ident: str, param_gen: ParamGenerator
    ) -> str:
        if isinstance(val, str):
            items = [item.strip() for item in val.split(",") if item.strip()]
            val = items
        elif not isinstance(val, list):
            val = [val]

        if not val:
            return "1=0" if op_str == "in" else "1=1"

        sql_op = "IN" if op_str == "in" else "NOT IN"
        placeholders = []
        for item in val:
            _, placeholder = param_gen.add("p", item)
            placeholders.append(placeholder)

        return f"{column_ident} {sql_op} ({', '.join(placeholders)})"

    def _handle_between(
        self, op_str: str, val: Any, column_ident: str, param_gen: ParamGenerator
    ) -> str:
        if not isinstance(val, list) or len(val) != 2:
            raise SQLGenerationError(
                f"Value must be a list of EXACTLY 2 items for operator '{op_str}'",
                context=val,
            )

        _, placeholder_start = param_gen.add("p", val[0])
        _, placeholder_end = param_gen.add("p", val[1])
        return f"{column_ident} BETWEEN {placeholder_start} AND {placeholder_end}"

    def _handle_standard_ops(
        self, sql_op: str, val: Any, column_ident: str, param_gen: ParamGenerator
    ) -> str:
        _, placeholder = param_gen.add("p", val)
        return f"{column_ident} {sql_op} {placeholder}"

    def _parse_condition(
        self,
        condition: FilterCondition,
        param_gen: ParamGenerator,
        column_metadata: Dict[str, Any] = None,
        force_agg: bool = False,
    ) -> str:
        """
        Parse a single FilterCondition into a SQL snippet and update param_gen.
        """
        column_ident = self._quote_identifier(condition.column)
        if force_agg:
            column_ident = f"MAX({column_ident})"
        op_str = str(
            condition.operator.value
            if hasattr(condition.operator, "value")
            else condition.operator
        ).lower()
        val = condition.value

        if op_str in ["is_null", "is_not_null", "is_empty", "is_not_empty"]:
            return self._handle_nullness(
                op_str, condition, column_ident, column_metadata
            )

        # Fix Falsy Zero Bug: Explicitly check against None and empty strings.
        if val is None or val == "":
            return "1=1"

        is_date_type = getattr(condition, "datatype", "string") == "date"
        if not is_date_type and column_metadata:
            # Case-insensitive lookup for metadata
            meta = column_metadata.get(condition.column)
            if not meta:
                # Try finding it if the key isn't exact case
                upper_col = condition.column.upper()
                for k, v in column_metadata.items():
                    if k.upper() == upper_col:
                        meta = v
                        break

            if meta:
                b_type = meta.get("base_type") if isinstance(meta, dict) else str(meta)
                if b_type == "date":
                    is_date_type = True

        if is_date_type:
            import datetime

            def parse_dt(v):
                if isinstance(v, str):
                    try:
                        if len(v) == 10:
                            return datetime.datetime.strptime(v, "%Y-%m-%d").date()
                        else:
                            return datetime.datetime.fromisoformat(
                                v.replace("Z", "+00:00")
                            )
                    except ValueError:
                        return v
                return v

            if isinstance(val, list):
                val = [parse_dt(v) for v in val]
            else:
                val = parse_dt(val)

        is_txt = self._is_text_type(condition, column_metadata)
        if (
            op_str
            in ["contains", "not_contains", "starts_with", "ends_with", "eq", "neq"]
            and is_txt
        ):
            return self._handle_text_wildcards(op_str, val, column_ident, param_gen)

        if op_str in ["in", "not_in"]:
            return self._handle_in_arrays(op_str, val, column_ident, param_gen)

        if op_str == "between":
            return self._handle_between(op_str, val, column_ident, param_gen)

        standard_ops = {
            "eq": "=",
            "neq": "!=",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
        }
        if op_str in standard_ops:
            return self._handle_standard_ops(
                standard_ops[op_str], val, column_ident, param_gen
            )

        raise SQLGenerationError(f"Unsupported operator: {op_str}", context=condition)

    def _parse_logical_group(
        self,
        group: LogicalGroup,
        param_gen: ParamGenerator,
        agg_aliases: set = None,
        column_metadata: Dict[str, Any] = None,
        force_agg: bool = False,
    ) -> Tuple[str, str]:
        """
        Recursively parses a LogicalGroup (AND/OR tree) into safe parameterized SQL WHERE and HAVING strings.
        Routes filters on aggregation columns to HAVING instead of WHERE.
        """
        if agg_aliases is None:
            agg_aliases = set()

        if not group.conditions:
            return "", ""

        where_snippets = []
        having_snippets = []

        # Determine if we need to promote this entire group to HAVING.
        # This is required if force_agg is True, or if this is an OR group containing aggregated filters.
        promo_needed = force_agg
        if not promo_needed and group.logic == "OR":
            for item in group.conditions:
                if self._is_aggregated(item, agg_aliases):
                    promo_needed = True
                    break

        for item in group.conditions:
            if isinstance(item, FilterCondition):
                if promo_needed:
                    sql = self._parse_condition(
                        item, param_gen, column_metadata, force_agg=True
                    )
                    having_snippets.append(f"({sql})")
                else:
                    sql = self._parse_condition(item, param_gen, column_metadata)
                    if item.column in agg_aliases:
                        having_snippets.append(f"({sql})")
                    else:
                        where_snippets.append(f"({sql})")

            elif isinstance(item, dict) and "column" in item:
                condition = FilterCondition(**item)
                if promo_needed:
                    sql = self._parse_condition(
                        condition, param_gen, column_metadata, force_agg=True
                    )
                    having_snippets.append(f"({sql})")
                else:
                    sql = self._parse_condition(condition, param_gen, column_metadata)
                    if condition.column in agg_aliases:
                        having_snippets.append(f"({sql})")
                    else:
                        where_snippets.append(f"({sql})")

            elif isinstance(item, LogicalGroup):
                w_sql, h_sql = self._parse_logical_group(
                    item,
                    param_gen,
                    agg_aliases,
                    column_metadata,
                    force_agg=promo_needed,
                )
                if w_sql:
                    where_snippets.append(f"({w_sql})")
                if h_sql:
                    having_snippets.append(f"({h_sql})")

            elif isinstance(item, dict) and "logic" in item:
                nested_group = LogicalGroup(**item)
                w_sql, h_sql = self._parse_logical_group(
                    nested_group,
                    param_gen,
                    agg_aliases,
                    column_metadata,
                    force_agg=promo_needed,
                )
                if w_sql:
                    where_snippets.append(f"({w_sql})")
                if h_sql:
                    having_snippets.append(f"({h_sql})")
            else:
                raise SQLGenerationError(
                    f"Invalid item in logical group: {type(item)}", context=item
                )

        logic_operator = f" {group.logic} "
        where_final = logic_operator.join(where_snippets) if where_snippets else ""
        having_final = logic_operator.join(having_snippets) if having_snippets else ""

        return where_final, having_final

    def _is_aggregated(self, item: Any, agg_aliases: set) -> bool:
        """Helper to check if a condition or group contains any aggregated filters."""
        if isinstance(item, FilterCondition):
            return item.column in agg_aliases
        if isinstance(item, dict):
            if "column" in item:
                return item["column"] in agg_aliases
            if "logic" in item:
                return any(
                    self._is_aggregated(c, agg_aliases)
                    for c in item.get("conditions", [])
                )
        if isinstance(item, LogicalGroup):
            return any(self._is_aggregated(c, agg_aliases) for c in item.conditions)
        return False

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

    def build_query(
        self, request: QueryRequest, is_count_query: bool = False
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Main entry point to assemble a full SECURE SELECT statement based on QueryRequest.
        Returns: (Full SQL Statement, Dict of bind parameters)
        """
        # 1. Alias Tracking
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
            if not is_count_query:
                raise SQLGenerationError(
                    "Aggressive Column Pruning strictly prohibits 'SELECT *' unbound memory payloads.",
                    context=request.columns,
                )
            # fallback for empty columns when counting
            select_clause = "1"

        # Collect Aggregation Aliases before pushdown
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

        param_gen = ParamGenerator()

        def resolve_dataset_source(ds_name: str, alias: str) -> str:
            ident = self._quote_identifier(ds_name)
            alias_ident = self._quote_identifier(alias)
            where_parts = []

            # 1. Partition predicate injection
            if request.partition_filters and ds_name in request.partition_filters:
                part_cfg = get_partition_config(ds_name)
                if part_cfg:
                    part_col_raw = part_cfg["load_id_column"].upper()
                    part_col = self._quote_identifier(part_col_raw)
                    part_values = request.partition_filters[ds_name]

                    # Detect base_type from column metadata
                    base_type = None
                    if request.column_metadata:
                        full_key = f"{ds_name}.{part_col_raw}".upper()
                        for k, v in request.column_metadata.items():
                            if k.upper() == full_key or k.upper() == part_col_raw:
                                base_type = v.get("base_type")
                                break

                    import datetime

                    def parse_val(v):
                        if base_type in ("date", "timestamp") and isinstance(v, str):
                            try:
                                if len(v) == 10:
                                    return datetime.datetime.strptime(
                                        v, "%Y-%m-%d"
                                    ).date()
                                else:
                                    return datetime.datetime.fromisoformat(
                                        v.replace("Z", "+00:00")
                                    )
                            except ValueError:
                                return v
                        elif base_type in ("number", "integer"):
                            try:
                                if isinstance(v, str) and "." not in v:
                                    return int(v)
                                return float(v)
                            except (ValueError, TypeError):
                                return v
                        return v

                    if len(part_values) == 1:
                        _, placeholder = param_gen.add(
                            f"part_{ds_name}", parse_val(part_values[0])
                        )
                        where_parts.append(f"{part_col} = {placeholder}")
                    elif len(part_values) > 1:
                        in_placeholders = []
                        for v in part_values:
                            _, placeholder = param_gen.add(
                                f"part_{ds_name}", parse_val(v)
                            )
                            in_placeholders.append(placeholder)
                        where_parts.append(
                            f"{part_col} IN ({', '.join(in_placeholders)})"
                        )

            # 2. User filter pushdown
            ds_filters = pushdown_map.get(ds_name)
            if ds_filters:
                w_sql, _ = self._parse_logical_group(
                    ds_filters, param_gen, agg_aliases, request.column_metadata
                )
                if w_sql:
                    where_parts.append(w_sql)

            # 3. Combine into subquery if any predicates exist
            if where_parts:
                combined = " AND ".join(where_parts)
                return f"(SELECT * FROM {ident} WHERE {combined}) {alias_ident}"
            return f"{ident} {alias_ident}"

        # Smart Join Ordering Heuristic
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

        # 2. FROM Clause
        base_source = resolve_dataset_source(request.dataset, base_alias)
        from_clause = f"FROM {base_source}"

        if request.joins:
            for i, join in enumerate(request.joins):
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

        # 3. Global WHERE/HAVING Clause
        having_sql = ""
        where_sql = ""
        if remaining_filters and getattr(remaining_filters, "conditions", None):
            w_sql, h_sql = self._parse_logical_group(
                remaining_filters, param_gen, agg_aliases, request.column_metadata
            )
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
        if request.sorting and len(request.sorting) > 0 and not is_count_query:
            sort_snippets = []
            for sort in request.sorting:
                col_ident = self._quote_identifier(sort.column)
                dir_sql = "DESC" if sort.direction == "DESC" else "ASC"
                sort_snippets.append(f"{col_ident} {dir_sql}")
            sql += f"\nORDER BY {', '.join(sort_snippets)}"

        # 6. LIMIT / OFFSET
        if not is_count_query:
            sql += (
                f"\nOFFSET {request.offset} ROWS FETCH NEXT {request.limit} ROWS ONLY"
            )

        return sql, param_gen.params

    def build_count_query(self, request: QueryRequest) -> Tuple[str, Dict[str, Any]]:
        """
        Builds a dedicated query specifically to fetch the total filtered row count.
        Avoids wrapping in a subquery to optimize performance on complex joins.
        """
        inner_sql, params = self.build_query(request, is_count_query=True)

        # If there are aggregations/group by, we MUST subquery to count the groups
        if request.group_by or request.aggregations:
            sql = f"SELECT COUNT(*) as total_rows FROM (\n{inner_sql}\n) sub"
            return sql, params

        # For non-grouped queries, we can replace the SELECT clause directly
        # Find the FROM keyword to strip out the dynamically generated SELECT
        from_idx = inner_sql.find("\nFROM ")
        if from_idx != -1:
            sql = f"SELECT COUNT(*) as total_rows {inner_sql[from_idx:]}"
            return sql, params

        # Fallback
        sql = f"SELECT COUNT(*) as total_rows FROM (\n{inner_sql}\n) sub"
        return sql, params
