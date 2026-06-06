import time
import xml.etree.ElementTree as ET
import psycopg2
import requests
import concurrent.futures
import argparse
import sys
import os
import csv
import threading
from datetime import datetime
import logging

class TeeStream:
    def __init__(self, filename, stream):
        self.stream = stream
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        self.file = open(filename, 'a', encoding='utf-8')

    def write(self, message):
        self.stream.write(message)
        self.file.write(message)
        self.file.flush()

    def flush(self):
        self.stream.flush()
        self.file.flush()

def setup_file_logging(log_filename):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, "logs")
    log_path = os.path.join(log_dir, log_filename)
    
    # Redirect sys.stdout and sys.stderr to write to both console and file
    sys.stdout = TeeStream(log_path, sys.stdout)
    sys.stderr = TeeStream(log_path, sys.stderr)

    # Configure root logger to write to sys.stdout (which is now redirected)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

setup_file_logging("generate_identifiers.log")

from config_loader import ConfigLoader


# Load config using multi-instance aware loader
# Prefer the new Python config if present, otherwise fall back to the old INI
config_path = 'config/config.py'
config_loader = ConfigLoader(config_path)
postgres_config = config_loader.get_postgres_config()
schema = postgres_config['schema']

# Validate configuration
is_valid, errors = config_loader.validate_instances()
if not is_valid:
    print("Configuration validation failed:")
    for error in errors:
        print(f"  - {error}")
    exit(1)

def postgres_connection():
    return psycopg2.connect(
        host=postgres_config['host'],
        user=postgres_config['user'],
        password=postgres_config['password'],
        dbname=postgres_config['database'],
        port=postgres_config['port']
    )

def handle_report_response(response_text, customer_account, report_name, instance_key):
    try:
        root = ET.fromstring(response_text)
        ns = {'ns1': 'http://www.collaboratemd.com/api/v1/'}
        status_elem = root.find('ns1:Status', ns)
        identifier_elem = root.find('ns1:Identifier', ns)
        status_message_elem = root.find('ns1:StatusMessage', ns)

        if status_elem is None or identifier_elem is None or status_message_elem is None:
            print(f"ERROR: Missing required XML elements in response for {report_name} - account {customer_account} - instance {instance_key}")
            return "ERROR", None

        status = status_elem.text
        identifier = identifier_elem.text
        status_message = status_message_elem.text
        print(f'identifier: {identifier} | StatusMessage: {status_message} | Instance: {instance_key}')

        if "still running" in status_message:
            return "RUNNING", identifier

        if status not in ('SUCCESS', 'REPORT RUNNING'):
            return False, None

        conn = postgres_connection()
        cur = conn.cursor()

        cur.execute(f"SELECT 1 FROM {schema}.account_reports WHERE customer_account = %s AND report_name = %s AND identifier = %s AND instance_key = %s",
                    (customer_account, report_name, identifier, instance_key))

        if cur.fetchone():
            print(f"Duplicate identifier {identifier} detected for {report_name} - account {customer_account} - instance {instance_key}")
            cur.close()
            conn.close()
            return "DUPLICATE", identifier

        cur.execute(f"UPDATE {schema}.account_reports SET status = 0 WHERE customer_account = %s AND report_name = %s AND status = 1 AND instance_key = %s",
                    (customer_account, report_name, instance_key))

        cur.execute(f"INSERT INTO {schema}.account_reports (customer_account, report_name, identifier, status, instance_key) VALUES (%s, %s, %s, 1, %s)",
                    (customer_account, report_name, identifier, instance_key))

        conn.commit()
        cur.close()
        conn.close()
        return True, identifier
    except ET.ParseError as e:
        print(f"ERROR: Failed to parse XML response for {report_name} - account {customer_account} - instance {instance_key}: {e}")
        return "ERROR", None
    except psycopg2.Error as e:
        print(f"ERROR: Database error for {report_name} - account {customer_account} - instance {instance_key}: {e}")
        return "ERROR", None
    except Exception as e:
        print(f"ERROR: Unexpected error handling report response for {report_name} - account {customer_account} - instance {instance_key}: {e}")
        return "ERROR", None


def record_attempt(instance_key, account, customer_name, report_name, report_id, http_status, api_status, identifier, status_message, retries, db_updated, results_list, results_lock):
    with results_lock:
        results_list.append({
            'instance_key': instance_key,
            'customer_account': account,
            'customer_name': customer_name,
            'report_name': report_name.upper(),
            'report_id': report_id,
            'http_status': http_status,
            'api_status': api_status,
            'identifier': str(identifier) if identifier is not None else 'None',
            'status_message': status_message,
            'retries': retries,
            'db_updated': 'TRUE' if db_updated else 'FALSE'
        })


def run_report_for_account(account, account_name, report_id, filter_id, report_name, instance_key, base_url, username, password, results_list, results_lock):
    """
    Runs the report for a single customer account, handles retries,
    updates the database, and records the attempt.
    """
    retries = 0
    while True:
        url = f"{base_url}/customer/{account}/reports/{report_id}/filter/{filter_id}/run"
        payload = f"<Run><Nonce>{time.time()}</Nonce></Run>"
        headers = {"Content-Type": "application/xml"}

        try:
            response = requests.post(url, data=payload, headers=headers, auth=(username, password))
            print(response.text)
            print(f"{report_name.upper()} | {report_id} | Status: {response.status_code} | Instance: {instance_key} | Account: {account_name} ({account})")

            status = "UNKNOWN"
            identifier = "None"
            status_message = "No response"
            db_updated = False

            if response.status_code == 200:
                result, identifier = handle_report_response(response.text, account, report_name, instance_key)
                
                try:
                    root = ET.fromstring(response.text)
                    ns = {'ns1': 'http://www.collaboratemd.com/api/v1/'}
                    status_elem = root.find('ns1:Status', ns)
                    status_message_elem = root.find('ns1:StatusMessage', ns)
                    if status_elem is not None:
                        status = status_elem.text
                    if status_message_elem is not None:
                        status_message = status_message_elem.text
                except Exception:
                    pass

                if result is True:
                    print(f"{report_name} report started and DB updated for account {account_name} ({account}) - instance {instance_key}")
                    db_updated = True
                    record_attempt(instance_key, account, account_name, report_name, report_id, response.status_code, status, identifier, status_message, retries, db_updated, results_list, results_lock)
                    break
                elif result == "RUNNING":
                    print(f"Report for {account_name} ({account}) ({instance_key}) is still running. Waiting 60 seconds before retrying...")
                    retries += 1
                    time.sleep(60)
                    continue
                elif result == "DUPLICATE":
                    print(f"Duplicate report identifier {identifier} returned {account_name} ({account}) ({instance_key}) - {report_name}. Waiting 60 seconds before retrying...")
                    retries += 1
                    time.sleep(60)
                    continue
                elif result == "ERROR":
                    print(f"Skipping {report_name} for account {account_name} ({account}) - instance {instance_key} due to error")
                    record_attempt(instance_key, account, account_name, report_name, report_id, response.status_code, "ERROR", identifier, status_message, retries, False, results_list, results_lock)
                    break
                else:
                    print(f"Failed to handle response for {report_name} - account {account_name} ({account}) - instance {instance_key}")
                    record_attempt(instance_key, account, account_name, report_name, report_id, response.status_code, "HANDLE_FAILED", identifier, status_message, retries, False, results_list, results_lock)
                    break
            else:
                print(f"API call failed for {report_name} - account {account_name} ({account}) - instance {instance_key} - Status: {response.status_code}")
                record_attempt(instance_key, account, account_name, report_name, report_id, response.status_code, f"HTTP_{response.status_code}", "None", f"API Call Failed: HTTP {response.status_code}", retries, False, results_list, results_lock)
                break
        except Exception as e:
            print(f"Exception during report generation for {report_name} - account {account_name} ({account}) - instance {instance_key}: {e}")
            record_attempt(instance_key, account, account_name, report_name, report_id, 0, "EXCEPTION", "None", str(e), retries, False, results_list, results_lock)
            break

    time.sleep(5)


def generate_report_for_all_accounts(report_id, filter_id, report_name, instance_key, base_url, username, password, accounts, account_names, results_list, results_lock):
    """
    Iterates over all customer accounts to generate the report for each.
    """
    for account in accounts:
        account_name = account_names.get(account, account)
        run_report_for_account(
            account=account,
            account_name=account_name,
            report_id=report_id,
            filter_id=filter_id,
            report_name=report_name,
            instance_key=instance_key,
            base_url=base_url,
            username=username,
            password=password,
            results_list=results_list,
            results_lock=results_lock
        )


def write_runs_summary(results_list):
    """
    Writes the runs summary and updates the latest summary file.
    """
    if not results_list:
        return
        
    csv_dir = 'csv_files/run_summaries'
    os.makedirs(csv_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file = os.path.join(csv_dir, f"identifier_summary_{timestamp}.csv")
    latest_csv_file = os.path.join(csv_dir, "latest_identifier_summary.csv")
    
    fields = [
        'instance_key', 'customer_account', 'customer_name', 'report_name', 'report_id',
        'http_status', 'api_status', 'identifier', 'status_message',
        'retries', 'db_updated'
    ]
    
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(results_list)
        print(f"\n✓ Generated runs summary written to: {csv_file}")
    except Exception as e:
        print(f"\n✗ Failed to write run summary CSV: {e}")

    try:
        with open(latest_csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(results_list)
        print(f"✓ Copied latest runs summary to: {latest_csv_file}")
    except Exception as e:
        print(f"✗ Failed to write latest summary CSV: {e}")



def run_all_reports(max_workers=None):
    """
    Generate reports for all configured instances.
    Runs each instance in parallel using a thread pool. Per-instance behavior is unchanged (reports are run for each account sequentially).
    """
    instances = config_loader.get_instances()
    instance_list = config_loader.list_instances()

    print(f"\n{'=' * 80}")
    print(f"GENERATE IDENTIFIERS - MULTI-INSTANCE MODE")
    print(f"{'=' * 80}")
    print(f"Processing {len(instance_list)} instance(s): {', '.join(instance_list)}\n")

    # Determine worker count
    if max_workers is None:
        max_workers = min(32, max(1, len(instance_list)))

    # List to collect results from all threads
    results_list = []
    results_lock = threading.Lock()

    def process_instance(instance_key):
        instance_config = instances[instance_key]
        print(f"\n{'=' * 80}")
        print(f"INSTANCE: {instance_key}")
        print(f"{'=' * 80}")
        print(f"API URL: {instance_config['api_base_url']}")
        print(f"Accounts: {instance_config['accounts']}\n")

        # Fetch per-instance report configs or fall back to global/legacy
        report_configs = config_loader.get_report_configs(instance_key=instance_key)

        for report_config in report_configs:
            generate_report_for_all_accounts(
                report_id=report_config["report_id"],
                filter_id=report_config["filter_id"],
                report_name=report_config["name"],
                instance_key=instance_key,
                base_url=instance_config['api_base_url'],
                username=instance_config['username'],
                password=instance_config['password'],
                accounts=instance_config['accounts'],
                account_names=instance_config.get('account_names', {}),
                results_list=results_list,
                results_lock=results_lock
            )

    # Run instances concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_instance = {executor.submit(process_instance, key): key for key in instance_list}
        for fut in concurrent.futures.as_completed(future_to_instance):
            key = future_to_instance[fut]
            try:
                fut.result()
            except Exception as e:
                print(f"Instance {key} raised an exception: {e}")

    # Write summary CSV
    write_runs_summary(results_list)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run generate_identifiers for multiple instances concurrently')
    parser.add_argument('--workers', '-w', type=int, default=None, help='Number of instances to process concurrently (default=len(instances) up to 32)')
    args = parser.parse_args()
    try:
        run_all_reports(max_workers=args.workers)
    except KeyboardInterrupt:
        print('\nInterrupted by user')
        sys.exit(1)
