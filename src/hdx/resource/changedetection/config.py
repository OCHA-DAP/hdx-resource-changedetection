import os

from hdx.utilities.easy_logging import setup_logging

# constants
LOOKUP = "hdx-resource-changedetection"
UPDATED_BY_SCRIPT = "HDX Resource Change Detection"


def init_logging():
    """Initialize logging from environment variables."""
    log_file_path = os.getenv("LOG_FILE_PATH", "hdx-resource-changedetection.log")
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    setup_logging(
        log_file=log_file_path,
        console_log_level=log_level,
        file_log_level=log_level,
    )
