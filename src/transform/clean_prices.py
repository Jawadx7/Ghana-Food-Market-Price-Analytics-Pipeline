import pandas as pd
from config.logger_config import setup_logger
from config.enums import LogType

logger = setup_logger("start_data_cleaning", LogType.TRANSFORMATION)

def remove_hxl_header(df: pd.DataFrame) -> pd.DataFrame:
    """Remove HXL tag row (row 0 with #date, #adm1+name, etc.)
    
    Args:
        df: Raw DataFrame with HXL tags in first row
        
    Returns:
        DataFrame without HXL tag row
    """
    logger.info("Checking for HXL tag header row")

    first_row_str = str(df.iloc[0, 0])
    if first_row_str.startswith('#'):
        logger.info("HXL tag row detected - removing row 0")
        df = df.iloc[1:].reset_index(drop=True)
        logger.info(f"DataFrame shape after removal: {df.shape}")
    else:
        logger.info("No HXL tag row found")
    
    return df
