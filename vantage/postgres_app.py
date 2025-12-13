from scripts.download_and_extract import download_and_extract
from scripts.load_extracted_data_psql import load_extracted_data_postgres
from configparser import ConfigParser
import logging
import os 
config = ConfigParser()
config.read('config/config.ini')

logging.basicConfig(filename='logs/loader.log', level=logging.INFO)

import os

def find_dat_folder(extract_root):
    for item in os.listdir(extract_root):
        item_path = os.path.join(extract_root, item)
        if os.path.isdir(item_path):
            for file in os.listdir(item_path):
                if file.lower().endswith(".dat"):
                    return item_path 
    return None  

def orchestrate():
    paths = download_and_extract()
    # get absolute path 
    absoulte_path = os.path.abspath(os.getcwd())
    
    for customer_id, extract_path in paths.items():
        logging.info(f"Starting Load for {customer_id} from {extract_path}")

        dat_folder = find_dat_folder(extract_path)

        print('...................',dat_folder)

        file_path = os.path.join(absoulte_path,extract_path,customer_id)        
        load_extracted_data_postgres(dat_folder)

if __name__ == "__main__":
    orchestrate()
