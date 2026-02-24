"""
Export Service — Background job manager for async Excel generation.

Manages the lifecycle of export jobs:
  - Submit a job → returns job_id immediately
  - Background thread writes Excel in constant_memory mode
  - Poll for status/progress
  - Download completed file

Thread-safe singleton pattern for use with FastAPI.
"""

import os
import uuid
import time
import threading
import tempfile
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# Directory for temporary export files
EXPORT_TEMP_DIR = os.path.join(tempfile.gettempdir(), "aurora_exports")
os.makedirs(EXPORT_TEMP_DIR, exist_ok=True)


@dataclass
class ExportJob:
    """Tracks the state of a single export job."""

    job_id: str
    status: str = "pending"  # pending | processing | complete | failed
    progress_pct: int = 0
    file_path: Optional[str] = None
    file_size_bytes: Optional[int] = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    dataset_name: str = ""
    estimated_rows: int = 0


class ExportService:
    """
    Thread-safe singleton managing async export jobs.
    Uses a bounded ThreadPoolExecutor to prevent CPU starvation.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._jobs: Dict[str, ExportJob] = {}
                # Bounded pool: max 16 concurrent exports to handle load from 200 users
                cls._instance._executor = ThreadPoolExecutor(
                    max_workers=16, thread_name_prefix="ExportWorker"
                )
            return cls._instance

    def submit_excel_job(
        self,
        sql: str,
        params: Dict[str, Any],
        dataset_name: str,
        columns: List[str],
        estimated_rows: int,
    ) -> ExportJob:
        """
        Submit an async Excel export job to the thread pool.
        Returns the ExportJob immediately; processing happens in background.
        """
        job_id = str(uuid.uuid4())[:12]
        job = ExportJob(
            job_id=job_id,
            dataset_name=dataset_name,
            estimated_rows=estimated_rows,
        )
        self._jobs[job_id] = job

        # Submit to bounded thread pool
        self._executor.submit(self._excel_worker, job, sql, params, columns)
        return job

    def get_job(self, job_id: str) -> Optional[ExportJob]:
        """Retrieve a job by ID."""
        return self._jobs.get(job_id)

    def cleanup_old_jobs(self, max_age_minutes: int = 30):
        """Delete stale temp files and job records."""
        cutoff = time.time() - (max_age_minutes * 60)
        stale_ids = [jid for jid, job in self._jobs.items() if job.created_at < cutoff]
        for jid in stale_ids:
            job = self._jobs.pop(jid, None)
            if job and job.file_path and os.path.exists(job.file_path):
                try:
                    os.remove(job.file_path)
                except OSError:
                    pass

    def _excel_worker(
        self,
        job: ExportJob,
        sql: str,
        params: Dict[str, Any],
        columns: List[str],
    ):
        """
        Background worker that writes Excel using xlsxwriter in constant_memory mode.
        Gets a connection from the Oracle connection pool to be thread-safe.
        """
        import xlsxwriter
        from app.db.factory import get_database_adapter

        job.status = "processing"
        temp_path = os.path.join(EXPORT_TEMP_DIR, f"{job.job_id}.xlsx")
        job.file_path = temp_path

        adapter = get_database_adapter()

        try:
            with adapter.connection() as conn:
                cursor = conn.cursor()

                # Execute the query
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                raw_names = [desc[0] for desc in cursor.description]
                col_names = [
                    name.split(".")[-1] if "." in name else name for name in raw_names
                ]

                # Create workbook in constant_memory mode (writes directly to disk)
                workbook = xlsxwriter.Workbook(
                    temp_path,
                    {"constant_memory": True, "nan_inf_to_errors": True},
                )
                worksheet = workbook.add_worksheet("Aurora Export")

                # Display enhancements
                worksheet.freeze_panes(1, 0)
                worksheet.set_default_row(15)
                worksheet.set_tab_color("#1e3a8a")

                if job.estimated_rows > 0:
                    worksheet.autofilter(0, 0, job.estimated_rows, len(col_names) - 1)

                # Styles — match UI: dark navy headers, white body, black text
                header_format = workbook.add_format(
                    {
                        "bold": True,
                        "font_color": "#ffffff",
                        "bg_color": "#0f172a",
                        "border": 1,
                        "border_color": "#0f172a",
                        "align": "left",
                        "valign": "vcenter",
                        "font_size": 11,
                        "font_name": "Calibri",
                    }
                )

                cell_format = workbook.add_format(
                    {
                        "border": 1,
                        "border_color": "#e2e8f0",
                        "bg_color": "#ffffff",
                        "font_color": "#000000",
                        "font_size": 10,
                        "font_name": "Calibri",
                    }
                )
                cell_format_alt = workbook.add_format(
                    {
                        "border": 1,
                        "border_color": "#e2e8f0",
                        "bg_color": "#f8fafc",
                        "font_color": "#000000",
                        "font_size": 10,
                        "font_name": "Calibri",
                    }
                )
                num_format = workbook.add_format(
                    {
                        "num_format": "#,##0.00",
                        "border": 1,
                        "border_color": "#e2e8f0",
                        "bg_color": "#ffffff",
                        "font_color": "#000000",
                        "font_size": 10,
                        "font_name": "Calibri",
                    }
                )
                num_format_alt = workbook.add_format(
                    {
                        "num_format": "#,##0.00",
                        "border": 1,
                        "border_color": "#e2e8f0",
                        "bg_color": "#f8fafc",
                        "font_color": "#000000",
                        "font_size": 10,
                        "font_name": "Calibri",
                    }
                )
                int_format = workbook.add_format(
                    {
                        "num_format": "#,##0",
                        "border": 1,
                        "border_color": "#e2e8f0",
                        "bg_color": "#ffffff",
                        "font_color": "#000000",
                        "font_size": 10,
                        "font_name": "Calibri",
                    }
                )
                int_format_alt = workbook.add_format(
                    {
                        "num_format": "#,##0",
                        "border": 1,
                        "border_color": "#e2e8f0",
                        "bg_color": "#f8fafc",
                        "font_color": "#000000",
                        "font_size": 10,
                        "font_name": "Calibri",
                    }
                )
                date_format = workbook.add_format(
                    {
                        "num_format": "yyyy-mm-dd hh:mm:ss",
                        "border": 1,
                        "border_color": "#e2e8f0",
                        "bg_color": "#ffffff",
                        "font_color": "#000000",
                        "font_size": 10,
                        "font_name": "Calibri",
                    }
                )
                date_format_alt = workbook.add_format(
                    {
                        "num_format": "yyyy-mm-dd hh:mm:ss",
                        "border": 1,
                        "border_color": "#e2e8f0",
                        "bg_color": "#f8fafc",
                        "font_color": "#000000",
                        "font_size": 10,
                        "font_name": "Calibri",
                    }
                )

                # Write headers
                for col_num, col_name in enumerate(col_names):
                    worksheet.write(0, col_num, col_name, header_format)

                # Write data in chunks
                row_idx = 1
                total_written = 0
                chunk_size = 10000

                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break

                    for row in rows:
                        is_alt = row_idx % 2 == 0
                        for col_idx, value in enumerate(row):
                            cf = cell_format_alt if is_alt else cell_format
                            # Handle special types
                            if value is None:
                                worksheet.write_blank(row_idx, col_idx, "", cf)
                            elif hasattr(value, "isoformat"):
                                worksheet.write_datetime(
                                    row_idx,
                                    col_idx,
                                    value,
                                    date_format_alt if is_alt else date_format,
                                )
                            elif isinstance(value, bool):  # Put bool check before int
                                worksheet.write_boolean(row_idx, col_idx, value, cf)
                            elif isinstance(value, int):
                                worksheet.write_number(
                                    row_idx,
                                    col_idx,
                                    value,
                                    int_format_alt if is_alt else int_format,
                                )
                            elif isinstance(value, float):
                                import math

                                if math.isnan(value) or math.isinf(value):
                                    worksheet.write_blank(row_idx, col_idx, "", cf)
                                else:
                                    worksheet.write_number(
                                        row_idx,
                                        col_idx,
                                        value,
                                        num_format_alt if is_alt else num_format,
                                    )
                            else:
                                worksheet.write_string(row_idx, col_idx, str(value), cf)
                        row_idx += 1

                    total_written += len(rows)

                    # Update progress
                    if job.estimated_rows > 0:
                        job.progress_pct = min(
                            99, int((total_written / job.estimated_rows) * 100)
                        )

                # Set stretched column widths
                worksheet.set_column(0, len(col_names) - 1, 22)
                workbook.close()
                cursor.close()

                # Finalize
                job.progress_pct = 100
                job.status = "complete"
                job.file_size_bytes = os.path.getsize(temp_path)
                logger.info(
                    f"Export job {job.job_id} complete: {total_written} rows → {temp_path}"
                )

        except Exception as e:
            job.status = "failed"
            job.error = str(e)
            logger.error(f"Export job {job.job_id} failed: {e}")
            # Clean up partial file
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass


# Module-level singleton
export_service = ExportService()
