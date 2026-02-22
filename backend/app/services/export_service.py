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
                # Bounded pool: max 4 concurrent exports to protect DB and memory
                cls._instance._executor = ThreadPoolExecutor(
                    max_workers=4, thread_name_prefix="ExportWorker"
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

                col_names = [desc[0] for desc in cursor.description]

                # Create workbook in constant_memory mode (writes directly to disk)
                workbook = xlsxwriter.Workbook(
                    temp_path,
                    {"constant_memory": True, "nan_inf_to_errors": True},
                )
                worksheet = workbook.add_worksheet("Aurora Export")

                # Premium header format
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
                        for col_idx, value in enumerate(row):
                            # Handle special types
                            if value is None:
                                worksheet.write_blank(row_idx, col_idx, None)
                            elif hasattr(value, "isoformat"):
                                worksheet.write_string(
                                    row_idx, col_idx, value.isoformat()
                                )
                            elif isinstance(value, (int, float)):
                                import math

                                if math.isnan(value) or math.isinf(value):
                                    worksheet.write_blank(row_idx, col_idx, None)
                                else:
                                    worksheet.write_number(row_idx, col_idx, value)
                            else:
                                worksheet.write_string(row_idx, col_idx, str(value))
                            col_idx += 1
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
