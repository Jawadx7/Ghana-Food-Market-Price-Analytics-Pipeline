import logging
from pathlib import Path
from typing import Optional
from config.enums import LogType  # Change from relative import

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)-20s | %(funcName)-15s | %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Maps Enum → logfile name
LOG_FILE_MAP = {
    LogType.PIPELINE: LOG_DIR / "pipeline.log",
    LogType.EXTRACTION: LOG_DIR / "extract.log",
    LogType.TRANSFORMATION: LOG_DIR / "transform.log",
    LogType.LOADING: LOG_DIR / "load.log",
}

def setup_logger(name: str, log_type: LogType) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)

    component_file = LOG_FILE_MAP[log_type]
    component_handler = logging.FileHandler(component_file)
    component_handler.setFormatter(formatter)
    logger.addHandler(component_handler)

    # Master pipeline.log always receives logs
    pipeline_file = LOG_FILE_MAP[LogType.PIPELINE]
    pipeline_handler = logging.FileHandler(pipeline_file)
    pipeline_handler.setFormatter(formatter)
    logger.addHandler(pipeline_handler)

    # Also print to terminal
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger