from config.logger_config import setup_logger
from config.enums import LogType
from pathlib import Path
from dotenv import load_dotenv
import os

# methods imports
from extract.csv_extractor import extract_prices
from transform.transform_prices import transform_prices

# setups and initializations
load_dotenv()
logger = setup_logger("start_project_pipeline", LogType.PIPELINE)


def main():    
    logger.info("========== DE Pipeline Started ==========")
    raw_prices_path = os.getenv("RAW_PRICES_PATH")

    extracted_prices_df = extract_prices(Path(raw_prices_path))

    if extracted_prices_df is None:
        logger.error("Extraction failed")
        return

    logger.info(f"File extraction successful for : {raw_prices_path}")

    # print(extracted_prices_df.isnull().sum())
    # print(extracted_prices_df.head())

    transformed_prices_df = transform_prices(extracted_prices_df)
    print(transformed_prices_df.dtypes)
    # print(transformed_prices_df['category'].nunique())
    # print(transformed_prices_df['category'].unique())


if __name__ == "__main__":
    main()