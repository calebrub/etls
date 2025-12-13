import requests, zipfile, io, os
from configparser import ConfigParser
import logging
import ast
from datetime import datetime
import base64
import zipfile
import logging
import requests
import ast
import xml.etree.ElementTree as ET


config = ConfigParser()
config.read('config/config.ini')
logging.basicConfig(filename='logs/loader.log', level=logging.INFO)


def convert_to_dat_line(row,column_order):
    return '|'.join(str(row.get(col, '')) for col in column_order)

def fetch_and_generate_dat():
    base_url = config['API']['report_api_base_url']
    username = config['API']['username']
    password = config['API']['password']
    customers = ast.literal_eval(config['CUSTOMERS']['accounts'])
    report_identifiers = ast.literal_eval(config['REPORTS']['identifiers'])
    report_names = dict(config.items('REPORT_NAMES'))
    
    dat_dir = 'dat_files'
    os.makedirs(dat_dir, exist_ok=True)

    for report_id in report_identifiers:
        report_data_lines = []
        for customer_id in customers:
            url = f"{base_url}/customer/{customer_id}/reports/results/{report_id}"
            response = requests.post(url, auth=(username, password))
    
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                data_element = root.find('Data')
         
                if data_element is not None and data_element.text:
                    base64_data = data_element.text
                    zip_bytes = base64.b64decode(base64_data)

                    zip_file_path = os.path.join(dat_dir, f"{report_id}_{customer_id}.zip")
                    with open(zip_file_path, 'wb') as f:
                        f.write(zip_bytes)
            else:
                logging.error(f"Failed to fetch {report_id} for {customer_id}: {response.status_code}")

        # Write all customer data to a single dat file per report
        file_name = f"{report_names.get(report_id, report_id)}.dat"
        with open(os.path.join(dat_dir, file_name), 'w') as f:
            for line in report_data_lines:
                f.write(line + '\n')
                
        logging.info(f"DAT file written: {report_id}.dat")
