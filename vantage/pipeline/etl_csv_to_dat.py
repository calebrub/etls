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
import psycopg2


config = ConfigParser()
config.read('config/config.ini')
# logging.basicConfig(filename='logs/loader.log', level=logging.INFO)

def postgres_connection():
    return psycopg2.connect(
        host=config['POSTGRES']['host'],
        user=config['POSTGRES']['user'],
        password='Reve#2025',
        dbname=config['POSTGRES']['database'],
        port=config['POSTGRES']['port']
    )

def convert_to_dat_line(row,column_order):
    return '|'.join(str(row.get(col, '')) for col in column_order)

# select * from 	account_reports ar where status=1 ;

def load_report_matrix_from_db():
    conn = postgres_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT customer_account, report_name, identifier 
        FROM account_reports 
        WHERE status = 1
    """)

    report_matrix = {}
    for customer_account, report_name, identifier in cursor.fetchall():
        report_matrix.setdefault(customer_account, {})[report_name] = identifier

    cursor.close()
    conn.close()

    return report_matrix


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

                    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
                        for zip_info in zip_file.infolist():
                            if zip_info.filename.endswith('.csv'):
                                with zip_file.open(zip_info) as csv_file:
                                    lines = csv_file.read().decode('utf-8').splitlines()
                                    headers = [h.strip() for h in lines[0].split(',')]

                                    for line in lines[1:]:
                                        row_values = [v.strip() for v in line.split(',')]
                                        row_dict = dict(zip(headers, row_values))
                                        dat_line = convert_to_dat_line(row_dict, headers)
                                        report_data_lines.append(dat_line)
            else:
                logging.error(f"Failed to fetch {report_id} for {customer_id}: {response.status_code}")

        # Save DAT file
        if report_data_lines:
            file_name = f"{report_names.get(report_id, report_id)}.dat"
            file_path = os.path.join(dat_dir, file_name)
            with open(file_path, 'w') as f:
                for line in report_data_lines:
                    f.write(line + '\n')
            logging.info(f"DAT file written: {file_name}")
