import requests
import ast
import xml.etree.ElementTree as ET
import psycopg2
from configparser import RawConfigParser
import time

# Load config
config = RawConfigParser()
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
        password=config['POSTGRES']['password'],
        dbname=config['POSTGRES']['database'],
        port=config['POSTGRES']['port']
    )

def handle_report_response(response_text, customer_account, report_name):
    root = ET.fromstring(response_text)
    ns = {'ns1': 'http://www.collaboratemd.com/api/v1/'}
    status = root.find('ns1:Status', ns).text
    identifier = root.find('ns1:Identifier', ns).text
    status_message = root.find('ns1:StatusMessage', ns).text
    print('identifier', identifier, "| StatusMessage", status_message)

    if "still running" in status_message:
        return "RUNNING", identifier

    if status not in ('SUCCESS', 'REPORT RUNNING'):
        return False, None

    conn = postgres_connection()
    cur = conn.cursor()

    cur.execute(f"SELECT 1 FROM {schema}.account_reports WHERE customer_account = %s AND report_name = %s AND identifier = %s",
                (customer_account, report_name, identifier))

    if cur.fetchone():
        print(f"Duplicate identifier {identifier} detected for {report_name} - account {customer_account}")
        cur.close()
        conn.close()
        return "DUPLICATE", identifier

    cur.execute(f"UPDATE {schema}.account_reports SET status = 0 WHERE customer_account = %s AND report_name = %s AND status = 1",
                (customer_account, report_name))

    cur.execute(f"INSERT INTO {schema}.account_reports (customer_account, report_name, identifier, status) VALUES (%s, %s, %s, 1)",
                (customer_account, report_name, identifier))

    conn.commit()
    cur.close()
    conn.close()
    return True, identifier

def generate_report_for_all_accounts(report_id, filter_id, report_name):
    for account in accounts:
        while True:
            url = f"{base_url}/customer/{account}/reports/{report_id}/filter/{filter_id}/run?ts={int(time.time())}"
            payload = f"<Run><Nonce>{time.time()}</Nonce></Run>"
            headers = {"Content-Type": "application/xml"}

            response = requests.post(url, data=payload, headers=headers, auth=(username, password))
            print(f"{report_name.upper()} | {report_id} | filter | {filter_id} | Status: {response.status_code}")

            if response.status_code == 200:
                result, identifier = handle_report_response(response.text, account, report_name)
                if result == True:
                    print(f"{report_name} report started and DB updated for account {account}")
                    break
                elif result == "RUNNING":
                    print(f"Report for {account} is still running. Waiting 60 seconds before retrying...")
                    time.sleep(60)
                    continue
                elif result == "DUPLICATE":
                    print(f"Duplicate report identifier {identifier} already exists. Skipping...")
                    break
                else:
                    print(f"Failed to handle response for {report_name} - account {account}")
                    break
            else:
                print(f"API call failed for {report_name} - account {account} - Status: {response.status_code}")
                break

            time.sleep(40)
        time.sleep(30)

def run_all_reports():
    report_configs = [
        {"report_id": "10078378", "filter_id": "10141925", "name": "ar_aging"},
        {"report_id": "10078486", "filter_id": "10141929", "name": "gross_billing"},
        {"report_id": "10078375", "filter_id": "10141926", "name": "charges_on_hold"},
        {"report_id": "10078446", "filter_id": "10141927", "name": "claim_stage_breakdown"},
        {"report_id": "10078463", "filter_id": "10141928", "name": "denial_trends"},
        {"report_id": "10078516", "filter_id": "10141930", "name": "payment_trend"},
        {"report_id": "10066805", "filter_id": "10141935", "name": "rcm_productivity"},
        {"report_id": "10078520", "filter_id": "10141934", "name": "user_time_spread"},
        {"report_id": "10078521", "filter_id": "10141933", "name": "write_off_trend"},
        {"report_id": "10078522", "filter_id": "10141931", "name": "pdr3_calculator"},
        {"report_id": "10078523", "filter_id": "10141932", "name": "rev_rec_charges"},
        {"report_id": "10078524", "filter_id": "10141937", "name": "rev_rec_payments"}
    ]

    for config in report_configs:
        generate_report_for_all_accounts(config["report_id"], config["filter_id"], config["name"])

if __name__ == "__main__":
    run_all_reports()
