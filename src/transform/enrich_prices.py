from config.logger_config import setup_logger
from config.enums import LogType
import pandas as pd
from datetime import datetime

logger = setup_logger("start_data_enrichment", LogType.TRANSFORMATION)


def enrich_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich price data with additional metadata and features

    Args:
        df: Cleaned DataFrame from transformation step

    Returns:
        Enriched DataFrame with additional features
    """

    logger.info("========== STARTING PRICE ENRICHMENT PIPELINE ==========")

    start_time = datetime.now()
    original_shape = df.shape
    logger.info(f"Input shape: {original_shape[0]:,} rows x {original_shape[1]} columns")

    # Step 1: ...


    duration = (datetime.now() - start_time).total_seconds()
    final_shape = df.shape

    logger.info("========== ENRICHMENT COMPLETE ==========")
    logger.info(f"Output shape: {final_shape[0]:,} rows x {final_shape[1]} columns")
    logger.info(f"Duration: {duration} seconds")
    logger.info(f"Columns added: {final_shape[1] - original_shape[1]}")

    return df