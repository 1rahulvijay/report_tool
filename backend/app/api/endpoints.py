from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import StreamingResponse
from typing import Literal, Any

from app.db.base import BaseDatabaseAdapter
from app.db.factory import get_database_adapter
from app.schemas.metadata import (
    DatasetListResponse,
    DatasetColumnsResponse,
    PartitionInfo,
)
from app.schemas.query import QueryRequest, PreviewResponse, RawQueryRequest
from app.core.partition_config import get_partition_config
from app.services.query_builder import QueryBuilderService, SQLGenerationError
from app.services.export_service import export_service
from app.core.config import get_settings
from app.core.rate_limit import limiter
from app.core.logger import logger
from app.core.table_config import (
    get_table_display_name,
    get_column_config,
    get_column_display_name,
)


router = APIRouter()


# Dependency
def get_db():
    db = get_database_adapter()
    yield db


def get_query_builder(settings=Depends(get_settings)):
    return QueryBuilderService()


def _parse_iso_dates(data: Any) -> Any:
    """
    Recursively traverse a dictionary or list and convert ISO 8601 strings
    to Python datetime objects.
    """
    from datetime import datetime

    if isinstance(data, dict):
        return {k: _parse_iso_dates(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_parse_iso_dates(v) for v in data]
    elif isinstance(data, str):
        # Attempt to parse ISO format (e.g., 2022-09-26T00:00:00)
        try:
            # Handle standard ISO format from frontend
            if len(data) >= 10 and data[4] == "-" and data[7] == "-":
                return datetime.fromisoformat(data.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass
    return data


@router.get("/datasets", response_model=DatasetListResponse)
def get_datasets(db: BaseDatabaseAdapter = Depends(get_db)):
    """
    Dynamically discover all available datasets in the attached database.
    """
    try:
        datasets = db.get_datasets()
        # Enrich each dataset with a user-friendly display name from table_config.json
        for ds in datasets:
            ds["display_name"] = get_table_display_name(ds["name"])
        return DatasetListResponse(datasets=datasets)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/datasets/{dataset_name:path}/columns", response_model=DatasetColumnsResponse
)
def get_dataset_columns(dataset_name: str, db: BaseDatabaseAdapter = Depends(get_db)):
    """
    Dynamically fetch column metadata (types, filterability) for a specific dataset.
    """
    try:
        columns = db.get_table_metadata(dataset_name)
        if not columns:
            raise ValueError(f"Dataset '{dataset_name}' not found or is empty.")

        # Apply column whitelist and friendly names from table_config.json
        col_cfg = get_column_config(dataset_name)
        if col_cfg:
            # Create a case-insensitive lookup for the config map
            cfg_lookup = {k.upper(): v for k, v in col_cfg.items()}

            # Only include columns that are in the whitelist
            filtered_columns = []
            for col in columns:
                key = col["name"].upper()
                if key in cfg_lookup:
                    col["name"] = key  # Force canonical name to UPPERCASE
                    col["display_name"] = cfg_lookup[key].get(
                        "display_name", col["name"]
                    )
                    filtered_columns.append(col)
            columns = filtered_columns
        else:
            # No whitelist — show all columns, add display_name = column name
            for col in columns:
                col["name"] = col["name"].upper()  # Force canonical name to UPPERCASE
                col["display_name"] = get_column_display_name(dataset_name, col["name"])

        # Check for partition configuration and fetch available values
        partition_info = None
        part_cfg = get_partition_config(dataset_name)
        part_data = None
        if part_cfg:
            try:
                if part_cfg.get("load_id_column"):
                    part_data = db.get_partition_values(
                        dataset_name,
                        part_cfg["load_id_column"],
                        load_type_column=part_cfg.get("load_type_column"),
                    )

                partition_info = PartitionInfo(
                    load_type_column=part_cfg.get("load_type_column"),
                    load_id_column=part_cfg.get("load_id_column"),
                    date_column=part_cfg.get("date_column"),
                    supported_types=part_cfg.get("supported_types", []),
                    available_values=part_data["values"] if part_data else [],
                    available_values_map=part_data.get("values_map")
                    if part_data
                    else None,
                    max_value=part_data["max_value"] if part_data else None,
                    min_value=part_data["min_value"] if part_data else None,
                )
            except Exception as e:
                # If partition query fails (e.g., column doesn't exist yet), skip
                logger.warning(
                    f"Partition query failed for {dataset_name}",
                    extra={"error": str(e)},
                )
                pass

        return DatasetColumnsResponse(
            dataset_name=dataset_name,
            columns=columns,
            partition_info=partition_info,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query/preview", response_model=PreviewResponse)
@limiter.limit(get_settings().PREVIEW_RATE_LIMIT)
async def preview_query(
    request: Request,
    query_request: QueryRequest,
    db: BaseDatabaseAdapter = Depends(get_db),
    builder: QueryBuilderService = Depends(get_query_builder),
    settings=Depends(get_settings),
):
    """
    Generate and execute a dynamic, parameterized ad-hoc analytical query securely.
    """
    import time
    import asyncio
    from app.core.rate_limit import check_concurrency, release_concurrency

    # 1. Enforce per-user analytical concurrency guard (max 2)
    check_concurrency(request)

    try:
        start_time = time.time()

        # Enforce preview row limit
        if (
            query_request.limit is None
            or query_request.limit > settings.PREVIEW_MAX_ROWS
        ):
            logger.info(
                f"Capping preview limit from {query_request.limit} to {settings.PREVIEW_MAX_ROWS}"
            )
            query_request.limit = settings.PREVIEW_MAX_ROWS

        sql, params = builder.build_query(query_request)
        count_sql, count_params = builder.build_count_query(query_request)

        # Cost Interception Safeguard
        if hasattr(db, "explain_query"):
            try:
                db.explain_query(sql, params)
            except ValueError as ve:
                raise HTTPException(status_code=400, detail=str(ve))

        # Enforce Query Timeout
        # Execute data fetch and count fetch concurrently to cut execution time
        try:
            data_coro = asyncio.wait_for(
                asyncio.to_thread(db.execute_query, sql, params),
                timeout=settings.QUERY_TIMEOUT_SECONDS,
            )
            count_coro = asyncio.wait_for(
                asyncio.to_thread(db.execute_query, count_sql, count_params),
                timeout=settings.QUERY_TIMEOUT_SECONDS,
            )
            data, count_data = await asyncio.gather(data_coro, count_coro)
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=408,
                detail=f"Query Execution Timeout: The query took longer than {settings.QUERY_TIMEOUT_SECONDS} seconds to complete. Please add more filters.",
            )

        total_rows = 0
        if count_data:
            first_row = count_data[0]
            total_rows = first_row.get("total_rows", first_row.get("TOTAL_ROWS", 0))

        # Determine actual selected columns
        actual_cols = list(data[0].keys()) if data else (query_request.columns or [])
        execution_time = round((time.time() - start_time) * 1000, 2)

        return PreviewResponse(
            dataset_name=query_request.dataset,
            total_row_count=total_rows,
            execution_time_ms=execution_time,
            data=data,
            columns=actual_cols,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Query Execution Error",
            extra={
                "error": str(e),
                "sql": sql if "sql" in locals() else "N/A",
                "dataset": query_request.dataset,
                "params": "*** REDACTED ***",
            },
            exc_info=True,
        )
        error_msg = str(e)
        if (
            "table or view does not exist" in error_msg.lower()
            or "not found" in error_msg.lower()
        ):
            raise HTTPException(
                status_code=404,
                detail=f"Dataset {query_request.dataset} not found in database.",
            )

        raise HTTPException(
            status_code=500, detail=f"Database Execution Error: {error_msg}"
        )
    finally:
        # Always release the concurrency slot so the user can query again
        release_concurrency(request)


@router.post("/query/raw")
async def execute_raw_query(
    request: RawQueryRequest,
    db: BaseDatabaseAdapter = Depends(get_db),
    settings=Depends(get_settings),
):
    """
    Execute a raw SQL query (used for presets).
    """
    try:
        # Robust date parsing for Oracle parameters
        params = _parse_iso_dates(request.params) if request.params else None

        import asyncio

        data = await asyncio.wait_for(
            asyncio.to_thread(db.execute_query, request.sql, params),
            timeout=settings.QUERY_TIMEOUT_SECONDS,
        )
        return {"data": data}
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=408,
            detail=f"Preset Query Timeout: Took longer than {settings.QUERY_TIMEOUT_SECONDS}s.",
        )
    except Exception as e:
        error_msg = str(e)
        if (
            "table or view does not exist" in error_msg.lower()
            or "not found" in error_msg.lower()
        ):
            raise HTTPException(
                status_code=404, detail="Dataset not found in database."
            )
        raise HTTPException(
            status_code=500, detail=f"Preset Execution Error: {error_msg}"
        )


@router.get("/debug/settings")
def get_debug_settings(settings=Depends(get_settings)):
    """Diagnostic endpoint to reveal runtime configuration (disabled in production)."""
    if settings.ENVIRONMENT == "production":
        raise HTTPException(
            status_code=403, detail="Debug endpoints are disabled in production."
        )

    import os

    return {
        "db_engine": settings.DB_ENGINE,
        "cwd": os.getcwd(),
    }


@router.post("/query/export")
@limiter.limit(get_settings().EXPORT_RATE_LIMIT)
def export_query(
    request: Request,
    format: Literal["csv", "excel"],
    query_request: QueryRequest,
    db: BaseDatabaseAdapter = Depends(get_db),
    builder: QueryBuilderService = Depends(get_query_builder),
    settings=Depends(get_settings),
):
    """
    Directly streams dataset matching filters as .csv or .xlsx from memory.
    Enforces strictly synchronous execution (no disk persistence).
    """
    try:
        # Enforce analytical concurrency guard (exports count as heavy analytical tasks)
        from app.core.rate_limit import check_concurrency, release_concurrency

        check_concurrency(request)

        export_request = query_request.model_copy()
        export_request.limit = settings.MAX_ROW_LIMIT
        export_request.offset = 0

        sql, params = builder.build_query(export_request)
        count_sql, count_params = builder.build_count_query(query_request)
    except SQLGenerationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        # Cost Interception Safeguard
        if hasattr(db, "explain_query"):
            try:
                db.explain_query(sql, params)
            except ValueError as ve:
                raise HTTPException(status_code=400, detail=str(ve))

        # 1. Row count for governance
        count_data = db.execute_query(count_sql, count_params)
        row_count = (
            count_data[0].get("total_rows", count_data[0].get("TOTAL_ROWS", 0))
            if count_data
            else 0
        )

        # 2. Hard limit for synchronous Excel to prevent OOM
        if format == "excel" and row_count > settings.EXPORT_EXCEL_MAX_ROWS:
            raise HTTPException(
                status_code=400,
                detail=f"Excel exports are limited to {settings.EXPORT_EXCEL_MAX_ROWS} rows due to memory safety. Please use CSV or add filters.",
            )

        # 3. Stream Response
        if format == "csv":
            response = StreamingResponse(
                export_service.stream_csv(sql, params), media_type="text/csv"
            )
            response.headers["Content-Disposition"] = (
                f"attachment; filename={query_request.dataset}_export.csv"
            )
            return response

        elif format == "excel":
            buffer = export_service.stream_excel(sql, params)

            def iter_buffer():
                yield buffer.getvalue()
                buffer.close()

            response = StreamingResponse(
                iter_buffer(),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            response.headers["Content-Disposition"] = (
                f"attachment; filename={query_request.dataset}_export.xlsx"
            )
            return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Export Streaming Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database Export Error: {str(e)}")
    finally:
        release_concurrency(request)
