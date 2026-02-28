"""
Export Service — Strictly synchronous in-memory streaming for Excel and CSV.
Eliminates all local file persistence and background workers.
"""

import io
import logging
from typing import Dict, Any, Iterator
import xlsxwriter

logger = logging.getLogger(__name__)


class ExportService:
    """
    Handles generation of export files in-memory using StreamingResponse-compatible generators.
    """

    def stream_csv(self, sql: str, params: Dict[str, Any]) -> Iterator[str]:
        """
        Executes query and yields CSV rows one by one to keep memory usage low.
        Uses the database adapter's execute_query_stream if possible, otherwise buffers.
        """
        from app.db.factory import get_database_adapter

        db = get_database_adapter()

        # We'll use a local connection to ensure the cursor stays open during iteration
        with db.connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            # Yield headers (strip schema/table prefix)
            headers = [desc[0].split(".")[-1] for desc in cursor.description]
            yield ",".join(f'"{h}"' for h in headers) + "\n"

            # Yield data rows
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    yield (
                        ",".join(f'"{str(v) if v is not None else ""}"' for v in row)
                        + "\n"
                    )

            cursor.close()

    def stream_excel(self, sql: str, params: Dict[str, Any]) -> io.BytesIO:
        """
        Generates an Excel file in-memory.
        Uses xlsxwriter's constant_memory mode to handle large datasets within RAM limits.
        """
        from app.db.factory import get_database_adapter

        db = get_database_adapter()
        output = io.BytesIO()

        with db.connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            # Get headers (strip schema/table prefix)
            headers = [desc[0].split(".")[-1] for desc in cursor.description]

            # Initialize workbook in constant_memory mode
            workbook = xlsxwriter.Workbook(
                output, {"constant_memory": True, "in_memory": True}
            )
            worksheet = workbook.add_worksheet("Aurora Export")

            # Header format
            header_format = workbook.add_format(
                {
                    "bold": True,
                    "font_color": "#ffffff",
                    "bg_color": "#0f172a",
                    "border": 1,
                }
            )

            # Write headers
            for col_num, col_name in enumerate(headers):
                worksheet.write(0, col_num, col_name, header_format)

            # Write data rows
            row_idx = 1
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    for col_idx, value in enumerate(row):
                        # Convert value to string to avoid issues with specialized DB types
                        worksheet.write(
                            row_idx, col_idx, str(value) if value is not None else ""
                        )
                    row_idx += 1

            workbook.close()
            cursor.close()

        output.seek(0)
        return output


# Singleton instance
export_service = ExportService()
