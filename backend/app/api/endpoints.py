from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Request
from fastapi.responses import StreamingResponse
from typing import Literal
import traceback
import io
import xlsxwriter

from app.db.base import BaseDatabaseAdapter
from app.db.factory import get_database_adapter
from app.schemas.metadata import (
    DatasetListResponse,
    DatasetColumnsResponse,
    PartitionInfo,
)
from app.schemas.query import QueryRequest, PreviewResponse
from app.schemas.export import ExportJobResponse, ExportStatusResponse, ExportStatus
from app.core.partition_config import get_partition_config
from app.services.query_builder import QueryBuilderService, SQLGenerationError
from app.services.export_service import export_service
from app.core.config import get_settings
from app.core.rate_limit import limiter


router = APIRouter()


# Dependency
def get_db():
    db = get_database_adapter()
    yield db


def get_query_builder(settings=Depends(get_settings)):
    return QueryBuilderService()


@router.get("/datasets", response_model=DatasetListResponse)
def get_datasets(db: BaseDatabaseAdapter = Depends(get_db)):
    """
    Dynamically discover all available datasets in the attached database.
    """
    try:
        datasets = db.get_datasets()
        return DatasetListResponse(datasets=datasets)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets/{dataset_name}/columns", response_model=DatasetColumnsResponse)
def get_dataset_columns(dataset_name: str, db: BaseDatabaseAdapter = Depends(get_db)):
    """
    Dynamically fetch column metadata (types, filterability) for a specific dataset.
    """
    try:
        columns = db.get_table_metadata(dataset_name)

        # Check for partition configuration and fetch available values
        partition_info = None
        part_cfg = get_partition_config(dataset_name)
        if part_cfg:
            try:
                part_data = db.get_partition_values(dataset_name, part_cfg["column"])
                partition_info = PartitionInfo(
                    column=part_cfg["column"],
                    load_type=part_cfg["load_type"],
                    available_values=part_data["values"],
                    max_value=part_data["max_value"],
                    min_value=part_data["min_value"],
                )
            except Exception as e:
                # If partition query fails (e.g., column doesn't exist yet), skip
                print(f"Partition query failed for {dataset_name}: {e}")
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
@limiter.limit("30/minute")  # Protect DB from rapid-fire query building
async def preview_query(
    request: Request,  # Required by slowapi
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

    start_time = time.time()

    try:
        sql, params = builder.build_query(query_request)
        count_sql, count_params = builder.build_count_query(query_request)
    except SQLGenerationError as e:
        raise HTTPException(status_code=400, detail=f"Query Generation Error: {str(e)}")

    try:
        # Cost Interception Safeguard
        if hasattr(db, "explain_query"):
            try:
                db.explain_query(sql, params)
            except ValueError as ve:
                raise HTTPException(status_code=400, detail=str(ve))

        # Enforce Query Timeout
        try:
            # db.execute_query is currently synchronous, so we run it in a threadpool to not block the event loop
            # and allow asyncio.wait_for to actually timeout the request.
            data = await asyncio.wait_for(
                asyncio.to_thread(db.execute_query, sql, params),
                timeout=settings.QUERY_TIMEOUT_SECONDS,
            )
            # Fetch the total count matching the actual filters
            count_data = await asyncio.wait_for(
                asyncio.to_thread(db.execute_query, count_sql, count_params),
                timeout=settings.QUERY_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=408,
                detail=f"Query Execution Timeout: The query took longer than {settings.QUERY_TIMEOUT_SECONDS} seconds to complete. Please add more filters.",
            )

        total_rows = (
            count_data[0].get("total_rows", count_data[0].get("TOTAL_ROWS", 0))
            if count_data
            else 0
        )

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
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Database Execution Error: {str(e)}"
        )


@router.get("/debug/settings")
def get_debug_settings(settings=Depends(get_settings)):
    """Diagnostic endpoint to reveal runtime configuration."""
    import os

    return {
        "db_engine": settings.DB_ENGINE,
        "cwd": os.getcwd(),
    }


@router.post("/query/export")
@limiter.limit("5/minute")  # Throttle heavy exports
def export_query(
    request: Request,  # Required by slowapi
    format: Literal["csv", "excel"],
    query_request: QueryRequest,
    db: BaseDatabaseAdapter = Depends(get_db),
    builder: QueryBuilderService = Depends(get_query_builder),
    settings=Depends(get_settings),
):
    """
    Export dataset matching filters as .csv or .xlsx.

    Data Governance Tiers:
      - ≤ 10k rows:   Sync Excel/CSV (immediate download)
      - 10k–100k rows: Async Excel (background job) / Sync CSV
      - > 100k rows:  Excel REJECTED (400), CSV streamed via cursor chunks

    Returns:
      - StreamingResponse for sync exports
      - ExportJobResponse JSON for async Excel jobs
    """
    try:
        export_request = query_request.model_copy()
        export_request.limit = settings.MAX_ROW_LIMIT  # Configurable safety limit
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

        # ── 2. Estimate row count for governance decisions ──
        count_data = db.execute_query(count_sql, count_params)
        row_count = (
            count_data[0].get("total_rows", count_data[0].get("TOTAL_ROWS", 0))
            if count_data
            else 0
        )

        # ── 3. Data Governance: reject xlsx > 100k rows ──
        if format == "excel" and row_count > settings.EXPORT_EXCEL_MAX_ROWS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Excel exports are limited to {settings.EXPORT_EXCEL_MAX_ROWS} rows. "
                    "Please apply filters to reduce the dataset, or export to CSV."
                ),
            )

        # ── 4. Route by format and size ──
        if format == "csv":
            if row_count > settings.EXPORT_EXCEL_MAX_ROWS:
                # Large CSV: stream via cursor chunks (never loads full DF)
                return _streaming_csv_response(db, sql, params, query_request.dataset)
            else:
                # Small/Medium CSV: sync DataFrame approach
                return _sync_csv_response(db, sql, params, query_request.dataset)

        elif format == "excel":
            if row_count > settings.EXPORT_EXCEL_ASYNC_ROWS:
                # Medium Excel: async background job
                columns = query_request.columns or []
                job = export_service.submit_excel_job(
                    sql=sql,
                    params=params,
                    dataset_name=query_request.dataset,
                    columns=columns,
                    estimated_rows=row_count,
                )
                return ExportJobResponse(
                    job_id=job.job_id,
                    status=ExportStatus.PENDING,
                    message=f"Export job queued for {row_count:,} rows. Poll /export/status/{job.job_id} for progress.",
                    estimated_rows=row_count,
                )
            else:
                # Small Excel: sync in-memory (existing behavior)
                return _sync_excel_response(db, sql, params, query_request.dataset)

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Database Export Error: {str(e)}")


@router.get("/export/status/{job_id}")
def get_export_status(job_id: str):
    """
    Poll the status of an async export job.
    Returns progress percentage and download URL when complete.
    """
    # Clean up stale jobs on each poll (cheap operation)
    export_service.cleanup_old_jobs(max_age_minutes=30)

    job = export_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=404, detail=f"Export job '{job_id}' not found or expired."
        )

    download_url = None
    if job.status == "complete":
        download_url = f"/api/v1/export/download/{job_id}"

    return ExportStatusResponse(
        job_id=job.job_id,
        status=ExportStatus(job.status),
        progress_pct=job.progress_pct,
        download_url=download_url,
        file_size_bytes=job.file_size_bytes,
        error=job.error,
    )


@router.get("/export/download/{job_id}")
def download_export(job_id: str, background_tasks: BackgroundTasks):
    """
    Serve the completed Excel file for download.
    File is deleted after serving (one-time download).
    """
    import os

    job = export_service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=404, detail=f"Export job '{job_id}' not found or expired."
        )

    if job.status != "complete":
        raise HTTPException(
            status_code=409,
            detail=f"Export job is still '{job.status}'. Please wait until complete.",
        )

    if not job.file_path or not os.path.exists(job.file_path):
        raise HTTPException(
            status_code=410, detail="Export file has expired or been deleted."
        )

    # Stream the file from disk
    def file_iterator():
        with open(job.file_path, "rb") as f:
            while chunk := f.read(65536):  # 64KB chunks
                yield chunk

    def cleanup_file():
        try:
            os.remove(job.file_path)
        except OSError:
            pass

    # Safely queue the file removal after the response is complete
    background_tasks.add_task(cleanup_file)

    response = StreamingResponse(
        file_iterator(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response.headers["Content-Disposition"] = (
        f"attachment; filename={job.dataset_name}_export.xlsx"
    )
    return response


# ═══════════════════════════════════════════════════════════
# Internal Export Helpers
# ═══════════════════════════════════════════════════════════


def _sync_csv_response(db, sql, params, dataset_name):
    """Small CSV: load DataFrame into memory and return."""
    df = db.execute_query_df(sql, params)
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = (
        f"attachment; filename={dataset_name}_export.csv"
    )
    return response


def _streaming_csv_response(db, sql, params, dataset_name):
    """
    Large CSV: stream via cursor chunks.
    Never materializes the full result set in memory.
    """

    def csv_generator():
        first_chunk = True
        for chunk in db.execute_query_cursor(sql, params, chunk_size=10000):
            if not chunk:
                continue
            if first_chunk:
                # Yield CSV header from the first row's keys
                header = ",".join(str(k) for k in chunk[0].keys())
                yield header + "\n"
                first_chunk = False
            for row in chunk:
                line = ",".join(_csv_escape(v) for v in row.values())
                yield line + "\n"

    response = StreamingResponse(csv_generator(), media_type="text/csv")
    response.headers["Content-Disposition"] = (
        f"attachment; filename={dataset_name}_export.csv"
    )
    return response


def _csv_escape(value) -> str:
    """Safely escape a value for CSV output."""
    if value is None:
        return ""
    s = str(value)
    if "," in s or '"' in s or "\n" in s:
        return '"' + s.replace('"', '""') + '"'
    return s


def _sync_excel_response(db, sql, params, dataset_name):
    """Small Excel (≤10k rows): build in-memory and return."""
    import numpy as np

    df = db.execute_query_df(sql, params)
    output = io.BytesIO()

    df = df.replace([np.inf, -np.inf], None)
    df = df.replace({np.nan: None})

    workbook = xlsxwriter.Workbook(
        output,
        {"constant_memory": True, "in_memory": True, "nan_inf_to_errors": True},
    )
    worksheet = workbook.add_worksheet("Aurora Export")

    header_format = workbook.add_format(
        {
            "bold": True,
            "font_color": "#ffffff",
            "bg_color": "#0f172a",
            "border": 1,
            "align": "center",
            "valign": "vcenter",
        }
    )

    col_names = df.columns.tolist()
    for col_num, value in enumerate(col_names):
        worksheet.write(0, col_num, value, header_format)

    for row_idx, row in enumerate(df.itertuples(index=False), start=1):
        for col_idx, value in enumerate(row):
            worksheet.write(row_idx, col_idx, value)

    worksheet.set_column(0, len(col_names) - 1, 22)
    workbook.close()
    output.seek(0)

    response = StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response.headers["Content-Disposition"] = (
        f"attachment; filename={dataset_name}_export.xlsx"
    )
    return response
