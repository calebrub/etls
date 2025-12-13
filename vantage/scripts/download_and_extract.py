import requests, zipfile, io, os
from configparser import ConfigParser
import logging
import ast
from datetime import datetime

config = ConfigParser()
config.read('config/config.ini')

logging.basicConfig(filename='logs/loader.log', level=logging.INFO)

def download_and_extract():
    url = config['API']['base_url']
    username = config['API']['username']
    password = config['API']['password']
    #download_path = config['PATHS']['download_dir']
    #extract_path = config['PATHS']['extract_dir']
    download_path = os.path.abspath("downloads")
    extract_path = os.path.abspath("extracted")
    
    # get list of account
    customers = ast.literal_eval(config['CUSTOMERS']['accounts'])    
    paths = {}
    
    for customer_id in customers:
        #url = f"https://webapi.collaboratemd.com/v2/customer/{customer_id}/snapshot"
        url = f"https://webapi.collaboratemd.com/v2/account/{customer_id}/snapshot"
        response = requests.get(url, auth=(username, password))

        if response.status_code == 200:
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            logging.info(f"Download Successful for {customer_id}")
            customer_download_path = os.path.join(download_path, f"{customer_id}_{timestamp}")
            customer_extract_path = os.path.join(extract_path, f"{customer_id}_{timestamp}")
            
            os.makedirs(customer_download_path, exist_ok=True)
            os.makedirs(customer_extract_path, exist_ok=True)
            zip_file_path = os.path.join(customer_download_path, f'snapshot_{timestamp}.zip')

            with open(zip_file_path, 'wb') as f:
                f.write(response.content)

            with zipfile.ZipFile(zip_file_path, 'r') as z:
                z.extractall(customer_extract_path)

            logging.info(f'Files extracted to {extract_path}/{customer_id}')
            logging.info(f'Files extracted to {customer_extract_path}')
            
            paths[customer_id] = customer_extract_path

        else:
            logging.error(f'Download failed for {customer_id} - Status: {response.status_code}')
        return paths