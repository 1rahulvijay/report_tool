"""Quick test to verify schema-qualified dataset names work in the query builder."""

import os, sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.services.query_builder import QueryBuilderService
from app.schemas.query import QueryRequest

# Test 1: Schema-qualified dataset in SELECT
req = QueryRequest(
    dataset="MGBCM.REAL_DATA_1",
    columns=["LOADID", "AS_OF_MONTH_SK", "LOAD_TYPE"],
    limit=10,
)
qb = QueryBuilderService()
sql, params = qb.build_query(req)
print("Test 1: Schema-qualified dataset")
print("SQL:", sql)
assert '"MGBCM"' in sql and '"REAL_DATA_1"' in sql, "Missing schema-qualified reference"
print("PASS\n")

# Test 2: Schema-qualified dataset with partition filter
req2 = QueryRequest(
    dataset="MGBCM.REAL_DATA_1",
    columns=["LOADID", "AS_OF_MONTH_SK"],
    partition_filters={"MGBCM.REAL_DATA_1": [202601]},
    limit=10,
)
sql2, params2 = qb.build_query(req2)
print("Test 2: Schema-qualified with partition filter")
print("SQL:", sql2)
print("Params:", params2)
assert "LOADID" in sql2, "Partition column missing"
print("PASS\n")

# Test 3: Schema-qualified JOIN
from app.schemas.query import JoinCondition, JoinOn, JoinType

req3 = QueryRequest(
    dataset="MGBCM.REAL_DATA_1",
    columns=["MGBCM.REAL_DATA_1.LOADID", "MGLOBE.REAL_DATA_2.LOADID"],
    joins=[
        JoinCondition(
            left_dataset="MGBCM.REAL_DATA_1",
            right_dataset="MGLOBE.REAL_DATA_2",
            join_type=JoinType.INNER,
            on=[JoinOn(left_column="LOADID", right_column="LOADID")],
        )
    ],
    limit=10,
)
sql3, params3 = qb.build_query(req3)
print("Test 3: Schema-qualified JOIN")
print("SQL:", sql3)
assert '"MGBCM"' in sql3 and '"MGLOBE"' in sql3, "Missing schema references in join"
print("PASS\n")

# Test 4: Partition config lookup - schema-qualified
from app.core.partition_config import get_partition_config, is_partitioned

cfg = get_partition_config("MGBCM.REAL_DATA_1")
assert cfg is not None, "Failed to find partition config for MGBCM.REAL_DATA_1"
assert cfg["load_id_column"] == "LOADID", (
    f"Wrong load_id_column: {cfg['load_id_column']}"
)
print("Test 4: Partition config lookup (schema-qualified)")
print("Config:", cfg)
print("PASS\n")

# Test 5: Partition config fallback - existing table without schema prefix
cfg2 = get_partition_config("AURORA_APP.employee_roster")
assert cfg2 is not None, "Fallback failed for AURORA_APP.employee_roster"
print("Test 5: Partition config fallback (schema prefix stripped)")
print("Config:", cfg2)
print("PASS\n")

# Test 6: Partition config - direct match without schema
cfg3 = get_partition_config("employee_roster")
assert cfg3 is not None, "Direct match failed for employee_roster"
print("Test 6: Partition config direct match (no schema)")
print("PASS\n")

print("=== ALL TESTS PASSED ===")
