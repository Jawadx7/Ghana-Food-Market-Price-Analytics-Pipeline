import pandas as pd
from config.logger_config import setup_logger
from config.enums import LogType
from pathlib import Path

logger = setup_logger("extract_prices", LogType.EXTRACTION)

def extract_prices(data_path: Path) -> pd.DataFrame:
    """Extracts the prices CSV file into a pandas DataFrame.

    :param data_path: the path to the raw prices csv file.

    :Returns:
        DataFrame on success, None on failure.

    >>> extracted_prices = extract_prices(example_file_path)
    print(extracted_prices.head())
    """
    
    try:
        logger.info(f"Reading file: {data_path}")
        return pd.read_csv(data_path)
    except FileNotFoundError:
        logger.error(f"File not found: {data_path}")
        return None
    except Exception:
        logger.exception(f"Unexpected error during extraction: {data_path}")
        return None