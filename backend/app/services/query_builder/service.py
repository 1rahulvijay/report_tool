from typing import Tuple, Dict, Any
import logging
from app.core.partition_config import get_partition_config
from app.schemas.query import (
    QueryRequest,
    FilterOperator,
)
from .base import SQLGenerationError, ParamGenerator
from .commons import CommonsMixin
from .filters import FilteringMixin

logger = logging.getLogger(__name__)


class QueryBuilderService(CommonsMixin, FilteringMixin):
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

    def _apply_alias(
        self, col_ref: str, alias_map: Dict[str, str], default_ds: str
    ) -> str:
        """Resolves a column reference to use its mapped alias."""
        if not alias_map:
            return col_ref
        full_name = col_ref if "." in col_ref else f"{default_ds}.{col_ref}"
        ds, cname = self._resolve_column_ref(full_name, alias_map, default_ds)
        if ds in alias_map:
            return f"{alias_map[ds]}.{cname}"
        return col_ref

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
            flat = ds_name.replace(".", "_")
            count = dataset_occurrences.get(flat, 0)
            dataset_occurrences[flat] = count + 1
            return flat if count == 0 else f"{flat}_{count}"

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
                        ds, cname = self._resolve_column_ref(
                            gb_col, alias_map, request.dataset
                        )
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
                res_agg_col = self._apply_alias(agg.column, alias_map, request.dataset)
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
                    ds, cname = self._resolve_column_ref(
                        full_name, alias_map, request.dataset
                    )
                    if ds in alias_map:
                        res_full_name = f"{alias_map[ds]}.{cname}"

                quoted = self._quote_identifier(res_full_name)
                quoted_cols.append(f'{quoted} AS "{full_name}"')
            select_clause = f"{hint}" + ", ".join(quoted_cols)
        else:
            if not is_count_query and not getattr(request, "is_preview", False):
                raise SQLGenerationError(
                    "Aggressive Column Pruning strictly prohibits 'SELECT *' unbound memory payloads.",
                    context=request.columns,
                )
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

            if request.partition_filters and ds_name in request.partition_filters:
                part_cfg = get_partition_config(ds_name)
                if part_cfg:
                    part_col_raw = part_cfg["load_id_column"].upper()
                    part_col = self._quote_identifier(part_col_raw)
                    part_values = request.partition_filters[ds_name]

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
                            f"part_{ds_name.replace('.', '_')}",
                            parse_val(part_values[0]),
                        )
                        where_parts.append(f"{part_col} = {placeholder}")
                    elif len(part_values) > 1:
                        in_placeholders = []
                        for v in part_values:
                            _, placeholder = param_gen.add(
                                f"part_{ds_name.replace('.', '_')}", parse_val(v)
                            )
                            in_placeholders.append(placeholder)
                        where_parts.append(
                            f"{part_col} IN ({', '.join(in_placeholders)})"
                        )

                    if request.partition_load_type and part_cfg.get("load_type_column"):
                        lt_col_name = part_cfg["load_type_column"].upper()
                        lt_col = self._quote_identifier(lt_col_name)
                        _, lt_placeholder = param_gen.add(
                            f"lt_{ds_name.replace('.', '_')}",
                            request.partition_load_type.upper(),
                        )
                        where_parts.append(f"UPPER({lt_col}) = {lt_placeholder}")

            ds_filters = pushdown_map.get(ds_name)
            if ds_filters:
                w_sql, _ = self._parse_logical_group(
                    ds_filters,
                    param_gen,
                    None,
                    ds_name,
                    agg_aliases,
                    request.column_metadata,
                )
                if w_sql:
                    where_parts.append(w_sql)

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
                    l_alias = alias_map.get(l_ds)
                    if not l_alias:
                        l_ds_resolved, l_col_name = self._resolve_column_ref(
                            cond.left_column, alias_map, l_ds
                        )
                        l_alias = alias_map.get(l_ds_resolved, l_ds_resolved)
                    else:
                        l_col_name = cond.left_column
                    if "." in l_col_name:
                        l_col_name = l_col_name.split(".")[-1]
                    left_full = f"{l_alias}.{l_col_name}"

                    r_col_name = cond.right_column
                    if "." in r_col_name:
                        r_col_name = r_col_name.split(".")[-1]
                    right_full = f"{right_alias}.{r_col_name}"

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
                remaining_filters,
                param_gen,
                alias_map,
                request.dataset,
                agg_aliases,
                request.column_metadata,
            )
            where_sql = w_sql
            having_sql = h_sql

        if where_sql:
            sql += f"\nWHERE {where_sql}"

        # 4. GROUP BY Clause
        if request.group_by and len(request.group_by) > 0:
            quoted_gb = []
            for c in request.group_by:
                res_c = self._apply_alias(c, alias_map, request.dataset)
                quoted_gb.append(self._quote_identifier(res_c))
            sql += f"\nGROUP BY {', '.join(quoted_gb)}"
            if having_sql:
                sql += f"\nHAVING {having_sql}"

        # 5. ORDER BY Clause
        if request.sorting and len(request.sorting) > 0 and not is_count_query:
            sort_snippets = []
            gb_cols = set(request.group_by) if request.group_by else set()

            for sort in request.sorting:
                # Oracle ORA-00979: Cannot ORDER BY columns not in GROUP BY if grouping
                if request.group_by and (
                    sort.column not in agg_aliases and sort.column not in gb_cols
                ):
                    continue

                if sort.column in agg_aliases:
                    col_ident = self._quote_identifier(sort.column)
                else:
                    res_col = self._apply_alias(sort.column, alias_map, request.dataset)
                    col_ident = self._quote_identifier(res_col)
                dir_sql = "DESC" if sort.direction == "DESC" else "ASC"
                sort_snippets.append(f"{col_ident} {dir_sql}")

            if sort_snippets:
                sql += f"\nORDER BY {', '.join(sort_snippets)}"

        # 6. LIMIT / OFFSET
        if not is_count_query:
            sql += (
                f"\nOFFSET {request.offset} ROWS FETCH NEXT {request.limit} ROWS ONLY"
            )

        logger.debug("FINAL SQL: %s", sql)
        return sql, param_gen.params

    def build_count_query(self, request: QueryRequest) -> Tuple[str, Dict[str, Any]]:
        """Builds a dedicated query specifically to fetch the total filtered row count."""
        inner_sql, params = self.build_query(request, is_count_query=True)
        if request.group_by or request.aggregations:
            sql = f'SELECT COUNT(*) as "total_rows" FROM (\n{inner_sql}\n) sub'
            return sql, params
        from_idx = inner_sql.find("\nFROM ")
        if from_idx != -1:
            sql = f'SELECT COUNT(*) as "total_rows" {inner_sql[from_idx:]}'
            return sql, params
        return f'SELECT COUNT(*) as "total_rows" FROM (\n{inner_sql}\n) sub', params
