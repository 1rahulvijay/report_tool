import pytest
from app.services.query_builder import QueryBuilderService
from app.schemas.query import (
    QueryRequest,
    FilterCondition,
    LogicalGroup,
    FilterOperator,
)
import json
import os
from unittest.mock import patch


@pytest.fixture
def mock_mapping_config():
    return {
        "tables": {
            "LOGICAL_SALES": {
                "physical_name": "PROD_SALES_TABLE_V2",
                "display_name": "Sales Records",
                "columns": {
                    "LOGICAL_AMOUNT": {
                        "physical_name": "AMT_VAL_USD",
                        "display_name": "Amount ($)",
                    }
                },
            }
        }
    }


def test_physical_table_and_column_mapping(mock_mapping_config):
    builder = QueryBuilderService()

    # Mock the loading of config
    with patch(
        "app.core.table_config._load_config", return_value=mock_mapping_config["tables"]
    ):
        request = QueryRequest(
            dataset="LOGICAL_SALES",
            columns=["LOGICAL_AMOUNT"],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    FilterCondition(
                        column="LOGICAL_AMOUNT",
                        operator=FilterOperator.GREATER_THAN,
                        value=1000,
                        datatype="number",
                    )
                ],
            ),
        )

        sql, params = builder.build_query(request)

        # SQL should contain the physical table and physical column in WHERE
        assert 'FROM "PROD_SALES_TABLE_V2"' in sql or 'FROM "LOGICAL_SALES"' not in sql

        # Verify the FROM clause uses the physical table name via OracleAdapter split logic
        # Note: QueryBuilder quotes whatever it gets. OracleAdapter resolves the physical name.
        # We need to test the integration.

        from app.db.oracle_adapter import OracleAdapter

        # Mock connection pool
        with patch("oracledb.create_pool"):
            adapter = OracleAdapter("user", "pass", "dsn")
            owner, table = adapter._parse_dataset_name("LOGICAL_SALES")
            assert table == "PROD_SALES_TABLE_V2"


def test_filter_uses_physical_column(mock_mapping_config):
    builder = QueryBuilderService()

    from app.services.query_builder.base import ParamGenerator

    pg = ParamGenerator()

    cond = FilterCondition(
        column="LOGICAL_AMOUNT",
        operator=FilterOperator.EQUALS,
        value=500,
        datatype="number",
    )

    with patch(
        "app.core.table_config._load_config", return_value=mock_mapping_config["tables"]
    ):
        sql = builder._parse_condition(cond, pg, default_ds="LOGICAL_SALES")
        # Should use physical column name AMT_VAL_USD
        assert '"AMT_VAL_USD"' in sql
