from config.logger_config import setup_logger
from config.enums import LogType
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
import os

# methods imports
from extract.csv_extractor import extract_prices

# setups and initializations
load_dotenv()
logger = setup_logger("start_project_pipeline", LogType.PIPELINE)


def main():    
    logger.info("========== Pipeline Started ==========")
    raw_prices_path = os.getenv("RAW_PRICES_PATH")

    prices_df = extract_prices(Path(raw_prices_path))

    if prices_df is None:
        logger.error("Extraction failed")
        return

    logger.info(f"File extraction successful: {raw_prices_path}")
    print(prices_df.shape)


if __name__ == "__main__":
    main()