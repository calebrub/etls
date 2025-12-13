# import sys;sys.path.append(r"c:\users\rkimera\appdata\local\programs\python\python313\lib\site-packages")
import os
import io
import zipfile
import base64
import requests
import logging
import ast
import xml.etree.ElementTree as ET
import psycopg2
from configparser import ConfigParser
import csv  

# Load config
config = ConfigParser()
config.read(r'C:\Users\rkimera\Desktop\reveloop\collboaratemd_project\config\config.ini')

# Logging setup
logging.basicConfig(filename=r'C:\Users\rkimera\Desktop\reveloop\collboaratemd_project\pipeline\logs\loader.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL connection
def postgres_connection():
    return psycopg2.connect(
        host=config['POSTGRES']['host'],
        user=config['POSTGRES']['user'],
        password='Reve#2025',
        dbname=config['POSTGRES']['database'],
        port=config['POSTGRES']['port']
    )

# Load report matrix from DB
def load_report_matrix_from_db():
    conn = postgres_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT customer_account, report_name, identifier 
        FROM {schema}.account_reports 
        WHERE status = 1
    """)

    report_matrix = {}
    for customer_account, report_name, identifier in cursor.fetchall():
        report_matrix.setdefault(customer_account, {})[report_name] = identifier

    cursor.close()
    conn.close()
    return report_matrix
   


# Convert a row dict to a pipe-separated line
def convert_to_dat_line(row, column_order):
    return '|'.join(str(row.get(col, '')) for col in column_order)


def fetch_and_generate_dat():
    base_url = config['API']['report_api_base_url']
    username = config['API']['username']
    password = config['API']['password']
    customers = ast.literal_eval(config['CUSTOMERS']['accounts'])

    dat_dir = 'dat_files'
    os.makedirs(dat_dir, exist_ok=True)

    report_matrix = load_report_matrix_from_db()

    # Identify all distinct report names
    all_report_names = set()
    for customer in customers:
        all_report_names.update(report_matrix.get(customer, {}).keys())
    print("All Reports: ", len(all_report_names), "Customer: ", len(customers))
    for report_name in all_report_names:
        report_data_lines = []
        headers_with_customer = []

        for customer_id in customers:
            report_id = report_matrix.get(customer_id, {}).get(report_name)
            if not report_id:
                logging.warning(f"No report ID found for {customer_id} - {report_name}")
                continue

            url = f"{base_url}/customer/{customer_id}/reports/results/{report_id}"
            response = requests.post(url, auth=(username, password))

            if response.status_code == 200:
                root = ET.fromstring(response.content)
                data_element = root.find('Data')
                #print(data_element)

                if data_element is not None and data_element.text:
                    zip_bytes = base64.b64decode(data_element.text)
                    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
                        for zip_info in zip_file.infolist():
                            if zip_info.filename.endswith('.csv'):
                                with zip_file.open(zip_info) as csv_file:
                                    decoded = io.TextIOWrapper(csv_file, encoding='utf-8')
                                    csv_reader = csv.reader(decoded)

                                    try:
                                        headers = next(csv_reader)
                                    except StopIteration:
                                        logging.warning(f"No data in CSV for customer {customer_id}")
                                        continue

                                    headers = [h.strip() for h in headers]
                                    headers_with_customer = ['customer_account'] + headers

                                    for row in csv_reader:
                                        row_values = [v.strip() for v in row]
                                        row_dict = dict(zip(headers, row_values))
                                        row_dict['customer_account'] = customer_id
                                        dat_line = convert_to_dat_line(row_dict, headers_with_customer)
                                        report_data_lines.append(dat_line)
            else:
                logging.error(f"Failed to fetch report {report_id} for customer {customer_id}: HTTP {response.status_code}")

        if report_data_lines:
            file_path = os.path.join(dat_dir, f"{report_name}.dat")
            with open(file_path, 'w') as f:
                for line in report_data_lines:
                    f.write(line + '\n')
            logging.info(f"DAT file written: {file_path}")
        else:
            logging.info(f"No data generated for report: {report_name}")



# Fetch reports and generate .dat files
def fetch_and_generate_dat_v1():
    base_url = config['API']['report_api_base_url']
    username = config['API']['username']
    password = config['API']['password']
    customers = ast.literal_eval(config['CUSTOMERS']['accounts'])
    
    dat_dir = 'dat_files'
    os.makedirs(dat_dir, exist_ok=True)

    report_matrix = load_report_matrix_from_db()

    # Identify all distinct report names
    all_report_names = set()
    for customer in customers:
        all_report_names.update(report_matrix.get(customer, {}).keys())

    for report_name in all_report_names:
        report_data_lines = []
        headers_with_customer = []

        for customer_id in customers:
            report_id = report_matrix.get(customer_id, {}).get(report_name)
            if not report_id:
                logging.warning(f"No report ID found for {customer_id} - {report_name}")
                continue

            url = f"{base_url}/customer/{customer_id}/reports/results/{report_id}"
            response = requests.post(url, auth=(username, password))

            if response.status_code == 200:
                root = ET.fromstring(response.content)
                data_element = root.find('Data')

                if data_element is not None and data_element.text:
                    zip_bytes = base64.b64decode(data_element.text)
                    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
                        for zip_info in zip_file.infolist():
                            if zip_info.filename.endswith('.csv'):
                                with zip_file.open(zip_info) as csv_file:
                                    lines = csv_file.read().decode('utf-8').splitlines()
                                    headers = [h.strip() for h in lines[0].split(',')]
                                    headers_with_customer = ['customer_account'] + headers

                                    for line in lines[1:]:
                                        row_values = [v.strip() for v in line.split(',')]
                                        row_dict = dict(zip(headers, row_values))
                                        row_dict['customer_account'] = customer_id
                                        dat_line = convert_to_dat_line(row_dict, headers_with_customer)
                                        report_data_lines.append(dat_line)
            else:
                logging.error(f"Failed to fetch report {report_id} for customer {customer_id}: HTTP {response.status_code}")

        if report_data_lines:
            file_path = os.path.join(dat_dir, f"{report_name}.dat")
            with open(file_path, 'w') as f:
                for line in report_data_lines:
                    f.write(line + '\n')
            logging.info(f"DAT file written: {file_path}")
        else:
            logging.info(f"No data generated for report: {report_name}")