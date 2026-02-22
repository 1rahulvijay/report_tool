"""
Export-related request/response schemas for the data governance pipeline.
"""

from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum


class ExportStatus(str, Enum):
    """Lifecycle states for an async export job."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETE = "complete"
    FAILED = "failed"


class ExportJobResponse(BaseModel):
    """Returned immediately when an async export job is created."""

    job_id: str = Field(..., description="Unique identifier for the export job")
    status: ExportStatus = Field(ExportStatus.PENDING, description="Initial job status")
    message: str = Field("", description="Human-readable status message")
    estimated_rows: int = Field(0, description="Estimated row count for the export")


class ExportStatusResponse(BaseModel):
    """Returned when polling /export/status/{job_id}."""

    job_id: str
    status: ExportStatus
    progress_pct: int = Field(0, ge=0, le=100, description="Export progress percentage")
    download_url: Optional[str] = Field(
        None, description="URL to download the file when complete"
    )
    file_size_bytes: Optional[int] = Field(
        None, description="Size of the generated file"
    )
    error: Optional[str] = Field(None, description="Error message if the job failed")
