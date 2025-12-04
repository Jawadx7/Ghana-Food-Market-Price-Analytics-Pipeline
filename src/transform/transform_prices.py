import pandas as pd
from config.logger_config import setup_logger
from config.enums import LogType
from datetime import datetime

# transform functions
from transform.clean_prices import remove_hxl_header
from transform.clean_prices import standardize_column_names
from transform.clean_prices import parse_dates
from transform.clean_prices import normalize_categories

logger = setup_logger("start_data_transformation", LogType.TRANSFORMATION)

def transform_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Main transformation pipeline for price data
    Orchestrates all cleaning and transformation steps
    
    Args:
        df: Raw DataFrame from extraction
        
    Returns:
        Cleaned and transformed DataFrame
    """
    logger.info("========== STARTING PRICE TRANSFORMATION PIPELINE ==========")
    
    start_time = datetime.now()
    original_shape = df.shape
    logger.info(f"Input shape: {original_shape[0]:,} rows x {original_shape[1]} columns")
    
    # Step 1: Remove HXL header
    df = remove_hxl_header(df)

    # Step 2: Standardize column names
    df = standardize_column_names(df)
    
    # Step 3: Parse dates
    df = parse_dates(df)

    # step 4: Normalize categories
    df = normalize_categories(df)
    
    # Step 4: Convert to numeric types
    
    # Step 5: Clean text columns
    
    # Step 6: Normalize units to per-kg
    
    # Step 7: Detect outliers
    
    # Step 8: Calculate data quality score
    
    # Remove temporary columns

    duration = (datetime.now() - start_time).total_seconds()
    final_shape = df.shape

    logger.info("========== TRANSFORMATION COMPLETE ==========")
    logger.info(f"Output shape: {final_shape[0]:,} rows x {final_shape[1]} columns")
    logger.info(f"Duration: {duration} seconds")
    logger.info(f"Columns added: {final_shape[1] - original_shape[1]}")

    return df