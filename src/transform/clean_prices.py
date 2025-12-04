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


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Clean column names to remove special characters and standardize
    
    Args:
        df: DataFrame with original column names
        
    Returns:
        DataFrame with standardized column names
    """
    logger.info("Standardizing column names")

    original_columns = df.columns.tolist()
    logger.debug(f"Original columns: {original_columns}")

    # Column mapping
    column_map = {
        'date': 'date',
        'admin1': 'region',
        'admin2': 'district',
        'market': 'market_name',
        'market_id': 'market_code',
        'latitude': 'latitude',
        'longitude': 'longitude',
        'category': 'category',
        'commodity': 'product',
        'commodity_id': 'product_code',
        'unit': 'unit',
        'priceflag': 'price_flag',
        'pricetype': 'price_type',
        'currency': 'currency',
        'price': 'price_ghs',
        'usdprice': 'price_usd'
    }

    df = df.rename(columns=column_map)

    new_columns = df.columns.tolist()
    logger.info(f"Renamed {len(column_map)} columns")
    logger.debug(f"New columns: {new_columns}")

    return df