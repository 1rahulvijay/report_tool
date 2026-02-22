import logging
import json
import os
import sys
from datetime import datetime


class JsonFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings after refining the record.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        # Add extra fields if provided via 'extra'
        if hasattr(record, "extra_fields"):
            log_record.update(record.extra_fields)

        return json.dumps(log_record)


def setup_logger(name: str = "aurora") -> logging.Logger:
    """
    Configures and returns a logger instance.
    Supports JSON formatting if LOG_FORMAT=json is set.
    """
    logger = logging.getLogger(name)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

    # Avoid duplicate handlers
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)

        if os.getenv("LOG_FORMAT", "text").lower() == "json":
            formatter = JsonFormatter()
        else:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# Global logger instance
logger = setup_logger()
