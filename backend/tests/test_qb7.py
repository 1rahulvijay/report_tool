import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Add backend dir to pythonpath
sys.path.insert(0, os.path.abspath("c:/Users/rahul/OneDrive/Documents/Aurora/backend"))

from app.services.query_builder import (
    QueryBuilderService,
    SQLGenerationError,
    ParamGenerator,
)
from app.schemas.query import QueryRequest, LogicalGroup, FilterCondition


class MyQB(QueryBuilderService):
    def _parse_logical_group_split(self, group, agg_aliases, param_gen):
        if not group.conditions:
            return "", "", param_gen.params

        where_snippets = []
        having_snippets = []
        all_params = {}

        for item in group.conditions:
            if isinstance(item, dict) and "column" in item:
                item = FilterCondition(**item)

            if isinstance(item, FilterCondition):
                sql = self._parse_condition(item, param_gen)

                # Check if this column targets an aggregation
                if item.column in agg_aliases:
                    having_snippets.append(f"({sql})")
                else:
                    where_snippets.append(f"({sql})")

            elif isinstance(item, LogicalGroup) or (
                isinstance(item, dict) and "logic" in item
            ):
                if isinstance(item, dict):
                    item = LogicalGroup(**item)

                w_sql, h_sql, _ = self._parse_logical_group_split(
                    item, agg_aliases, param_gen
                )

                if w_sql and h_sql and group.logic == "OR":
                    raise SQLGenerationError(
                        "Cannot mix raw and aggregated filters in an OR condition."
                    )

                if w_sql:
                    where_snippets.append(f"({w_sql})")
                if h_sql:
                    having_snippets.append(f"({h_sql})")

        where_final = ""
        having_final = ""

        if where_snippets:
            where_final = f" {group.logic} ".join(where_snippets)
        if having_snippets:
            having_final = f" {group.logic} ".join(having_snippets)

        return where_final, having_final, param_gen.params


qb = MyQB()
agg_aliases = {"Sum"}
filters = LogicalGroup(
    logic="AND",
    conditions=[
        FilterCondition(column="department", operator="eq", value="Sales"),
        FilterCondition(column="Sum", operator="gt", value=100, datatype="number"),
    ],
)

param_gen = ParamGenerator()
w, h, p = qb._parse_logical_group_split(filters, agg_aliases, param_gen)
print("WHERE:", w)
print("HAVING:", h)
print("PARAMS:", p)
