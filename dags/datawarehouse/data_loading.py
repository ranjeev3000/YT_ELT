import json, logging
from datetime import date

logger = logging.getLogger(__name__)

def load_path():
    file_path = f"./data/YT_data_{date.today()}.json"
    try:
        logger.info(f"Processing file: YT_data_{date.today()}")
        with open(file_path,"r", encoding='utf-8') as raw_data:
            data = json.load(raw_data)
        return data
    except FileNotFoundError as e:
        logger.error(f"File not found or Invalid Json in File: {file_path}")
        raise e