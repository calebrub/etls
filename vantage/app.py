from scripts.download_and_extract import download_and_extract
from scripts.load_extracted_data_mysql import load_extracted_data_mysql
from configparser import ConfigParser
import logging
import os 
config = ConfigParser()
config.read('config/config.ini')

logging.basicConfig(filename='logs/loader.log', level=logging.INFO)

def orchestrate():
    paths = download_and_extract()
    
    for customer_id, extract_path in paths.items():
        logging.info(f"Starting Load for {customer_id} from {extract_path}")
        
        file_path = os.path.join(extract_path,customer_id)
        
        load_extracted_data_mysql(file_path)

if __name__ == "__main__":
    orchestrate()
