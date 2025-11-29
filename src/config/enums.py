from enum import Enum

class LogType(Enum):
    PIPELINE = "pipeline"
    EXTRACTION = "extract"
    TRANSFORMATION = "transform"
    LOADING = "load"
