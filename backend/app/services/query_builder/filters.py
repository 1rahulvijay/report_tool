from typing import List, Dict, Any, Tuple
import datetime
import re
from app.schemas.query import LogicalGroup, FilterCondition
from .base import SQLGenerationError, ParamGenerator


class FilteringMixin:
    """Encapsulates recursive filter parsing and SQL operator handling."""

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
        self,
        op_str: str,
        val: Any,
        column_ident: str,
        param_gen: ParamGenerator,
        is_date: bool = False,
        is_numeric: bool = False,
    ) -> str:
        sql_op = "NOT LIKE" if op_str in ["not_contains", "neq"] else "LIKE"
        val_str = str(val)

        if is_numeric and "." in val_str:
            val_str = val_str.rstrip("0").rstrip(".")

        if op_str in ["contains", "not_contains"]:
            param_val = f"%{val_str}%"
        elif op_str == "starts_with":
            param_val = f"{val_str}%"
        elif op_str == "ends_with":
            param_val = f"%{val_str}"
        elif op_str in ["eq", "neq"]:
            param_val = f"{val_str}"

        _, placeholder = param_gen.add("p", param_val)

        if is_date:
            return f"TO_CHAR({column_ident}, 'YYYY-MM-DD HH24:MI:SS') {sql_op} UPPER({placeholder})"

        effective_column = column_ident
        if is_numeric:
            effective_column = (
                f"TO_CHAR({column_ident}, 'TM', 'NLS_NUMERIC_CHARACTERS=''. ''')"
            )
        elif not is_date:
            effective_column = f"CAST({column_ident} AS VARCHAR2(4000))"

        return f"UPPER({effective_column}) {sql_op} UPPER({placeholder})"

    def _handle_in_arrays(
        self,
        op_str: str,
        val: Any,
        column_ident: str,
        param_gen: ParamGenerator,
        is_txt: bool = False,
    ) -> str:
        if isinstance(val, str):
            items = [
                item.strip() for item in re.split(r"[,\t\n\r]+", val) if item.strip()
            ]
            val = items
        elif not isinstance(val, list):
            val = [val]

        if len(val) > 999:
            val = val[:999]

        if not val:
            return "1=0" if op_str == "in" else "1=1"

        sql_op = "IN" if op_str == "in" else "NOT IN"
        placeholders = []
        for item in val:
            if is_txt and isinstance(item, str):
                _, placeholder = param_gen.add("p", item.upper())
            else:
                _, placeholder = param_gen.add("p", item)
            placeholders.append(placeholder)

        if is_txt:
            return f"UPPER(CAST({column_ident} AS VARCHAR2(4000))) {sql_op} ({', '.join(placeholders)})"
        return f"{column_ident} {sql_op} ({', '.join(placeholders)})"

    def _handle_between(
        self, op_str: str, val: Any, column_ident: str, param_gen: ParamGenerator
    ) -> str:
        if not isinstance(val, list) or len(val) != 2:
            raise SQLGenerationError(
                f"Value must be 2 items for '{op_str}'", context=val
            )

        v_start, v_end = val[0], val[1]

        # Graceful degradation if user only provided one side of the 'between' filter
        if v_start and not v_end:
            _, placeholder = param_gen.add("p", v_start)
            return f"{column_ident} >= {placeholder}"
        elif not v_start and v_end:
            _, placeholder = param_gen.add("p", v_end)
            return f"{column_ident} <= {placeholder}"
        elif not v_start and not v_end:
            return "1=1"

        _, placeholder_start = param_gen.add("p", v_start)
        _, placeholder_end = param_gen.add("p", v_end)

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
        alias_map: Dict[str, Any] = None,
        default_ds: str = None,
        column_metadata: Dict[str, Any] = None,
        force_agg: bool = False,
    ) -> str:
        if alias_map:
            res_column = self._apply_alias(condition.column, alias_map, default_ds)
        else:
            col = condition.column
            if default_ds and col.upper().startswith(default_ds.upper() + "."):
                col = col[len(default_ds) + 1 :]
            if "." in col:
                col = col.rsplit(".", 1)[-1]
            res_column = col

        if not res_column or res_column.strip() == "":
            return "1=1"

        # Resolve physical column name if mapping exists
        from app.core.table_config import resolve_physical_column_name

        physical_col = resolve_physical_column_name(default_ds or "", res_column)

        column_ident = self._quote_identifier(physical_col)
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

        if val is None or val == "":
            return "1=1"

        is_date_type = getattr(condition, "datatype", "string") in ("date", "timestamp")
        if not is_date_type and column_metadata:
            upper_col = condition.column.upper()
            col_only = upper_col.split(".")[-1]
            meta = column_metadata.get(condition.column)
            if not meta:
                for k, v in column_metadata.items():
                    if k.upper() == upper_col or k.upper().split(".")[-1] == col_only:
                        meta = v
                        break
            if meta:
                m_type = str(meta.get("data_type", meta.get("base_type", ""))).lower()
                if any(t in m_type for t in ("date", "time", "stamp")):
                    is_date_type = True
                if any(
                    t in m_type
                    for t in ("numeric", "number", "float", "integer", "int")
                ):
                    condition.datatype = "number"

        if is_date_type:
            if op_str not in ["contains", "starts_with", "ends_with", "not_contains"]:
                import datetime

                def parse_single_dt(v):
                    if isinstance(v, str):
                        try:
                            if len(v) == 10:
                                return datetime.datetime.strptime(v, "%Y-%m-%d").date()
                            return datetime.datetime.fromisoformat(
                                v.replace("Z", "+00:00")
                            )
                        except Exception:
                            try:
                                from dateutil import parser

                                return parser.parse(
                                    v, default=datetime.datetime(2000, 1, 1)
                                )
                            except Exception:
                                return v
                    return v

                if isinstance(val, list):
                    val = [parse_single_dt(v) for v in val]
                else:
                    val = parse_single_dt(val)

        is_txt = self._is_text_type(condition, column_metadata)
        is_numeric = getattr(condition, "datatype", "string") == "number"

        if op_str in [
            "contains",
            "not_contains",
            "starts_with",
            "ends_with",
        ] and (is_txt or is_date_type or is_numeric):
            return self._handle_text_wildcards(
                op_str,
                val,
                column_ident,
                param_gen,
                is_date=is_date_type,
                is_numeric=is_numeric,
            )

        if op_str in ["eq", "neq"] and is_txt:
            _, placeholder = param_gen.add("p", str(val).upper())
            sql_op = "=" if op_str == "eq" else "!="
            return (
                f"UPPER(CAST({column_ident} AS VARCHAR2(4000))) {sql_op} {placeholder}"
            )

        if op_str in ["in", "not_in"]:
            return self._handle_in_arrays(op_str, val, column_ident, param_gen, is_txt)

        if op_str == "between":
            # For dates, TRUNC both sides so time components don't interfere
            if is_date_type:
                return self._handle_between(
                    op_str, val, f"TRUNC({column_ident})", param_gen
                )
            return self._handle_between(op_str, val, column_ident, param_gen)

        # Date-aware comparisons: TRUNC() for eq/neq/gt/lt to ignore time component
        if is_date_type and op_str in ("eq", "neq", "gt", "gte", "lt", "lte"):
            standard_ops = {
                "eq": "=",
                "neq": "!=",
                "gt": ">",
                "gte": ">=",
                "lt": "<",
                "lte": "<=",
            }
            _, placeholder = param_gen.add("p", val)
            return f"TRUNC({column_ident}) {standard_ops[op_str]} {placeholder}"

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
        alias_map: Dict[str, str] = None,
        default_ds: str = None,
        agg_aliases: set = None,
        column_metadata: Dict[str, Any] = None,
        force_agg: bool = False,
    ) -> Tuple[str, str]:
        if agg_aliases is None:
            agg_aliases = set()
        if not group.conditions:
            return "", ""

        where_snippets, having_snippets = [], []
        promo_needed = force_agg or (
            group.logic == "OR"
            and any(self._is_aggregated(c, agg_aliases) for c in group.conditions)
        )

        for item in group.conditions:
            cond = (
                item
                if isinstance(item, FilterCondition)
                else (
                    FilterCondition(**item)
                    if isinstance(item, dict) and "column" in item
                    else None
                )
            )
            if cond:
                sql = self._parse_condition(
                    cond,
                    param_gen,
                    alias_map,
                    default_ds,
                    column_metadata,
                    force_agg=promo_needed or cond.column in agg_aliases,
                )
                if promo_needed or cond.column in agg_aliases:
                    having_snippets.append(f"({sql})")
                else:
                    where_snippets.append(f"({sql})")
            else:
                l_group = (
                    item
                    if isinstance(item, LogicalGroup)
                    else (
                        LogicalGroup(**item)
                        if isinstance(item, dict) and "logic" in item
                        else None
                    )
                )
                if l_group:
                    w, h = self._parse_logical_group(
                        l_group,
                        param_gen,
                        alias_map,
                        default_ds,
                        agg_aliases,
                        column_metadata,
                        force_agg=promo_needed,
                    )
                    if w:
                        where_snippets.append(f"({w})")
                    if h:
                        having_snippets.append(f"({h})")
                else:
                    raise SQLGenerationError("Invalid item in logical group")

        logic = f" {group.logic} "
        return logic.join(where_snippets), logic.join(having_snippets)

    def _is_aggregated(self, item: Any, agg_aliases: set) -> bool:
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
        from app.schemas.query import FilterCondition, LogicalGroup

        if isinstance(item, dict):
            if "column" in item:
                item = FilterCondition(**item)
            elif "logic" in item:
                item = LogicalGroup(**item)

        if isinstance(item, FilterCondition):
            if item.column in agg_aliases:
                return None, item
            if item.column.startswith(f"{target_dataset}.") or (
                "." not in item.column and target_dataset == base_dataset
            ):
                return item, None
            return None, item

        if isinstance(item, LogicalGroup):
            if item.logic == "OR":
                all_p = all(
                    self._split_filters_for_dataset(
                        c, target_dataset, base_dataset, agg_aliases
                    )[1]
                    is None
                    for c in item.conditions
                )
                return (item, None) if all_p else (None, item)
            elif item.logic == "AND":
                p, r = [], []
                for c in item.conditions:
                    pc, rc = self._split_filters_for_dataset(
                        c, target_dataset, base_dataset, agg_aliases
                    )
                    if pc:
                        p.append(pc)
                    if rc:
                        r.append(rc)
                return (
                    LogicalGroup(logic="AND", conditions=p) if p else None,
                    LogicalGroup(logic="AND", conditions=r) if r else None,
                )
        return None, item
