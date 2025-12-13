import os
import io
import requests
import logging
import ast
import xml.etree.ElementTree as ET
import psycopg2
from configparser import ConfigParser
import time


# Load config
config = ConfigParser()
config.read('config/config.ini')
base_url = config['API']['report_api_base_url']
accounts = ast.literal_eval(config['CUSTOMERS']['accounts'])    
username = config['API']['username']
password = config['API']['password']
schema = config['POSTGRES']['schema']

def postgres_connection():
    return psycopg2.connect(
        host=config['POSTGRES']['host'],
        user=config['POSTGRES']['user'],
        password='Reve#2025',
        dbname=config['POSTGRES']['database'],
        port=config['POSTGRES']['port']
    )

def handle_report_response(response_text, customer_account, report_name):
    # Parse XML response
    root = ET.fromstring(response_text)
    ns = {'ns1': 'http://www.collaboratemd.com/api/v1/'}
    status = root.find('ns1:Status', ns).text
    identifier = root.find('ns1:Identifier', ns).text
    print('identifier',identifier)

    if status not in ('SUCCESS', 'REPORT RUNNING'):
        logging.error(f"Failed to generate report for {customer_account}, {report_name}")
        return False

    try:
        conn = postgres_connection()
        cur = conn.cursor()

        # Set existing records' status to 0
        cur.execute(f"""
            UPDATE {schema}.account_reports
            SET status = 0
            WHERE customer_account = %s AND report_name = %s AND status = 1
        """, (customer_account, report_name))

        # Insert new record
        cur.execute(f"""
                    INSERT INTO {schema}.account_reports (customer_account, report_name, identifier, status)
                    VALUES (%s, %s, %s, 1)
                """, (customer_account, report_name, identifier))

        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Inserted new report entry for {customer_account}, {report_name}")
        return True

    except Exception as e:
        logging.error(f"DB Error: {e}")
        return False



def generate_report_for_all_accounts(report_id, filter_id, report_name):
    for account in accounts:
        url = f"{base_url}/customer/{account}/reports/{report_id}/filter/{filter_id}/run"
        
        try:
            response = requests.post(url, auth=(username, password))
            print(f"{report_name.upper()} | {account} | Status: {response.status_code}")

            if response.status_code == 200:
                print(response.text)
                success = handle_report_response(response.text, account, report_name)
                if success:
                    print(f"{report_name} report started and DB updated for account {account}")
                else:
                    print(f"Failed to handle response for {report_name} - account {account}")
            else:
                print(f"API call failed for {report_name} - account {account} - Status: {response.status_code}")

        except Exception as e:
            logging.error(f"Exception for {report_name} - account {account}: {str(e)}")

        time.sleep(10)  # Delay to avoid duplicate identifiers


def run_all_reports():
    report_configs = [
        {"report_id": "10062054", "filter_id": "10137065", "name": "ar_aging"},
        {"report_id": "10062055", "filter_id": "10137067", "name": "charges_on_hold"},
        {"report_id": "10062056", "filter_id": "10137069", "name": "claim_stage_breakdown"},
        {"report_id": "10062057", "filter_id": "10137072", "name": "denial_trends"},
        {"report_id": "10062059", "filter_id": "10137074", "name": "gross_billing"},
        {"report_id": "10062060", "filter_id": "10137076", "name": "payment_trend"},
        {"report_id": "10062061", "filter_id": "10137077", "name": "quadrant_performance"},
        {"report_id": "10062064", "filter_id": "10137071", "name": "rcm_productivity"},
        {"report_id": "10062065", "filter_id": "10137078", "name": "user_time_spread"},
        {"report_id": "10062066", "filter_id": "10137079", "name": "write_off_trend"},
    ]

    for config in report_configs:
        generate_report_for_all_accounts(config["report_id"], config["filter_id"], config["name"])

run_all_reports()