import json
import logging
import sys
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        data: Dict[str, Any] = {
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
            "time": self.formatTime(record, self.datefmt),
        }
        if record.exc_info:
            data["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            data["stack_info"] = self.formatStack(record.stack_info)
        if hasattr(record, "extra_fields") and isinstance(
            record.extra_fields, dict
        ):
            data.update(record.extra_fields)
        return json.dumps(data, ensure_ascii=True)


def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler])


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

