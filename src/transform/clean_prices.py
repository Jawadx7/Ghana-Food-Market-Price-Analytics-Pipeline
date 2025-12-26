import pandas as pd
from config.logger_config import setup_logger
from config.enums import LogType
import re
import numpy as np

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


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Convert date column to datetime format
    Handles multiple date formats (MM/DD/YYYY, YYYY-MM-DD)
    
    Args:
        df: DataFrame with date column as string
        
    Returns:
        DataFrame with parsed date column
    """
    logger.info("Passing date columns")

    original_date_dtypes = df["date"].dtype
    logger.debug(f"Original date type: {original_date_dtypes}")

    # Parse dates - handles both formats
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    null_dates = df["date"].isnull().sum()

    if null_dates > 0:
        logger.warning(f"Found {null_dates} unparseable dates")
    
    logger.info(f"Date range: {df['date'].min()} - {df["date"].max()}")
    
    return df


def normalize_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Clean category values and remove wide-spaces

    Args:
        df: DataFrame with raw category values
    
    Returns:
        DataFrame with cleaned category values
    """

    logger.info("Start category normalization")

    original_categories = df['category'].unique()
    logger.info(f"Found {len(original_categories)} unique categories")
    logger.debug(f"Original categories: {original_categories}")

    df['category'] = (
        df['category']
        .str.lower()
        .str.strip()
        .str.replace(r"[^\w\s]", "", regex=True)
        .str.replace(r"\s+", "_", regex=True)
    )

    final_categories = df['category'].unique()
    logger.debug(f"Final categories: {final_categories}")

    categories = final_categories.tolist()
    df['category'] = pd.Categorical(df['category'], categories=categories, ordered=False)

    return df


def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert price, coordinate and other numeric columns to numeric types
    
    Args:
        df: DataFrame with string numeric columns
        
    Returns:
        DataFrame with proper numeric types
    """
    logger.info("Converting columns to numeric types")

    numeric_columns = {
        'price_ghs': 'float',
        'price_usd': 'float',
        'latitude': 'float',
        'longitude': 'float',
        'market_code': 'int',
        'product_code': 'int'
    }

    for col, dtype in numeric_columns.items():
        if col in df.columns:
            if dtype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce")
                df[col] = df[col].astype('Int64')
            else:
                logger.warning(f"Unknown dtype '{dtype}' for column '{col}', skipping conversion.")
        else:
            logger.warning(f"Column '{col}' not found in DataFrame")

    logger.info("Numeric conversion complete")

    return df


def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize text columns (trim whitespace, uppercase regions, etc.)
    
    Args:
        df: DataFrame with text columns
        
    Returns:
        DataFrame with cleaned text
    """
    logger.info("Cleaning text columns")

    text_columns = ["region", 'district', 'market_name', "product", "price_flag", "price_type", "currency"]

    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

            if col in ("region", "district"):
                df[col] = df[col].str.upper()
        else:
            logger.warning(f"Column '{col}' not found in DataFrame")

    return df


def normalize_units(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize unit values to standard units (e.g., per kg)

    Args:
        df: DataFrame with raw unit values

    Returns:
        DataFrame with normalized unit values
    """

    logger.info("Normalizing units to per-kg and dropping non-weight units")

    df = df.copy()

    # Standardize unit text
    df["unit"] = df["unit"].astype(str).str.strip().str.upper()

    # Extract numeric kg values ONLY for KG-based units
    def extract_kg(unit):
        match = re.match(r"(\d+)\s*KG", unit)
        return int(match.group(1)) if match else None

    df["unit_kg"] = df["unit"].apply(extract_kg)

    # Drop non-kg units (Bunch, Tubers, etc.)
    before = len(df)
    df = df[df["unit_kg"].notna()]
    after = len(df)

    logger.info(f"Dropped {before - after} rows with non-weight units")

    df["price_per_kg"] = df["price_ghs"] / df["unit_kg"]

    if (df["price_per_kg"] <= 0).any():
        logger.warning("Found non-positive price_per_kg values")
    
    return df
    

def detect_outliers(df, column='price_per_kg', std_threshold=3):
    """
    Detect price outliers using z-score method
    Flags prices that are > 3 standard deviations from mean
        
    Args:
        df: DataFrame with price data
        column: Column to check for outliers
        std_threshold: Number of standard deviations for outlier threshold
            
    Returns:
        DataFrame with is_outlier flag
    """
    logger.info(f"Detecting outliers in {column} (threshold: {std_threshold} std dev)")
        
    # Calculate z-scores by product and region (prices vary by product/region)
    df['z_score'] = df.groupby(['product', 'region'])[column].transform(
        lambda x: np.abs((x - x.mean()) / x.std())
    )
        
    # Flag outliers
    df['is_outlier'] = df['z_score'] > std_threshold
        
    outlier_count = df['is_outlier'].sum()
    outlier_pct = (outlier_count / len(df)) * 100
        
    logger.warning(f"Detected {outlier_count:,} outliers ({outlier_pct:.2f}% of data)")
        
    if outlier_count > 0:
        outlier_samples = df[df['is_outlier']][['date', 'market_name', 'product', 'price_per_kg', 'z_score']].head(5)
        logger.debug(f"Sample outliers:\n{outlier_samples.to_string()}")
        
    return df