import requests, zipfile, io, os
from configparser import ConfigParser
import logging
import ast
from datetime import datetime

config = ConfigParser()
config.read('config/config.ini')
logging.basicConfig(filename='logs/loader.log', level=logging.INFO)

from concurrent.futures import ThreadPoolExecutor
def fetch_and_generate_dat():
    base_url = config['API']['base_url']
    username = config['API']['username']
    password = config['API']['password']
    customers = ast.literal_eval(config['CUSTOMERS']['accounts'])
    report_identifiers = ast.literal_eval(config['REPORTS']['identifiers'])

    dat_dir = 'dat_files'
    os.makedirs(dat_dir, exist_ok=True)

    for report_id in report_identifiers:
        report_data_lines = []
        for customer_id in customers:
            url = f"{base_url}/customer/{customer_id}/reports/results/{report_id}"
            response = requests.get(url, auth=(username, password))

            if response.status_code == 200:
                data = response.json()  # assuming JSON response
                for row in data:
                    row['customer_id'] = customer_id
                    line = convert_to_dat_line(row)
                    report_data_lines.append(line)
            else:
                logging.error(f"Failed to fetch {report_id} for {customer_id}: {response.status_code}")

        # Write all customer data to a single dat file per report
        with open(os.path.join(dat_dir, f"{report_id}.dat"), 'w') as f:
            for line in report_data_lines:
                f.write(line + '\n')

        logging.info(f"DAT file written: {report_id}.dat")

def convert_to_dat_line(row):
    # convert dict to pipe-separated string or other format as required
    return '|'.join([str(row[k]) for k in sorted(row)])



from concurrent.futures import ThreadPoolExecutor

def fetch_report_for_customer(customer_id, report_id):
    url = f"{base_url}/customer/{customer_id}/reports/results/{report_id}"
    response = requests.get(url, auth=(username, password))
    if response.status_code == 200:
        return customer_id, response.json()
    else:
        logging.error(f"Failed {report_id} for {customer_id}: {response.status_code}")
        return customer_id, []

def generate_dat_files():
    with ThreadPoolExecutor(max_workers=10) as executor:
        for report_id in report_identifiers:
            futures = [executor.submit(fetch_report_for_customer, cid, report_id) for cid in customers]
            lines = []
            for f in futures:
                customer_id, data = f.result()
                for row in data:
                    row['customer_id'] = customer_id
                    line = convert_to_dat_line(row)
                    lines.append(line)
            with open(f'dat_files/{report_id}.dat', 'w') as f:
                f.writelines([line + '\n' for line in lines])


def fetch_report_for_customer(customer_id, report_id):
    url = f"{base_url}/customer/{customer_id}/reports/results/{report_id}"
    response = requests.get(url, auth=(username, password))
    if response.status_code == 200:
        return customer_id, response.json()
    else:
        logging.error(f"Failed {report_id} for {customer_id}: {response.status_code}")
        return customer_id, []

def generate_dat_files():
    with ThreadPoolExecutor(max_workers=10) as executor:
        for report_id in report_identifiers:
            futures = [executor.submit(fetch_report_for_customer, cid, report_id) for cid in customers]
            lines = []
            for f in futures:
                customer_id, data = f.result()
                for row in data:
                    row['customer_id'] = customer_id
                    line = convert_to_dat_line(row)
                    lines.append(line)
            with open(f'dat_files/{report_id}.dat', 'w') as f:
                f.writelines([line + '\n' for line in lines])



def report_api(report_identifier):
    url = config['API']['base_url']
    username = config['API']['username']
    password = config['API']['password']    
    # get list of account
    customers = ast.literal_eval(config['CUSTOMERS']['accounts'])    
    paths = {}
    
    for customer_id in customers:
        url = f"https://webapi.collaboratemd.com/v1/customer/{customer_id}/reports/results/{report_identifier}"
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