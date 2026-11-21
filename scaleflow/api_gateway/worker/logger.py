import logging
import json
import os
import sys
from datetime import datetime, timezone


class StructuredJsonFormatter(logging.Formatter):
    """
    Formats log records as single-line JSON objects for observability tools
    (Datadog, Loki, Elasticsearch, CloudWatch).
    """

    def __init__(self, service_name="scaleflow-worker"):
        super().__init__()
        self.service_name = service_name
        self.worker_id = os.getenv("WORKER_ID") or os.getenv("COMPUTERNAME") or "worker"

    def format(self, record):
        log_obj = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service_name,
            "worker_id": self.worker_id,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include custom contextual attributes if passed via extra
        context_keys = [
            "event",
            "task_id",
            "message_id",
            "outbox_id",
            "retry_count",
            "duration_ms",
            "reason",
            "error"
        ]
        for key in context_keys:
            if hasattr(record, key):
                log_obj[key] = getattr(record, key)

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj)


class StructuredTextFormatter(logging.Formatter):
    """
    Clean human-readable formatter for local development terminal.
    """

    def format(self, record):
        event_str = f"[{getattr(record, 'event', '')}] " if hasattr(record, "event") else ""
        task_str = f"[Task {getattr(record, 'task_id', '')}] " if hasattr(record, "task_id") else ""
        msg = f"{record.levelname:<5} {event_str}{task_str}{record.getMessage()}"
        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)
        return msg


def get_logger(name="scaleflow.worker"):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    log_format = os.getenv("LOG_FORMAT", "text").lower()

    if log_format == "json":
        handler.setFormatter(StructuredJsonFormatter())
    else:
        handler.setFormatter(StructuredTextFormatter())

    logger.addHandler(handler)
    logger.propagate = False
    return logger


# Default logger instance
logger = get_logger()
