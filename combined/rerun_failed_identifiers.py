"""
rerun_failed_identifiers.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Identifies failed report runs from a past summary CSV, loads the corresponding
instance credentials (even if currently commented out in config/config.py), and
reruns only those failed reports.

Usage:
    python rerun_failed_identifiers.py [--csv CSV_PATH] [--workers N] [--dry-run]
"""

import argparse
import concurrent.futures
import csv
import glob
import logging
import os
import sys
import threading

# Ensure we can import modules in this directory (combined)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import generate_identifiers
from logging_utils import setup_file_logging

# Setup file logging for the rerun script
setup_file_logging("rerun_failed_identifiers.log")

# ---------------------------------------------------------------------------
# Commented-out Config Parsing Helper
# ---------------------------------------------------------------------------

def load_full_instances_config(config_path: str = 'config/config.py') -> dict:
    """
    Reads config.py and parses it, selectively uncommenting commented-out
    instance dictionaries (e.g., 'ca' or 'reveloop') so that their credentials
    can be extracted.
    """
    if not os.path.exists(config_path):
        # Try relative to the script location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, config_path)
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found at: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    new_lines = []
    in_instances = False
    for line in lines:
        if line.strip().startswith('INSTANCES = {'):
            in_instances = True
            new_lines.append(line)
            continue
        
        if in_instances:
            stripped = line.strip()
            # If it is a commented-out line, decide whether to uncomment it
            if line.startswith('    #') or line.startswith('     #'):
                content = stripped.lstrip('#').strip()
                # Skip comments that are descriptive sentences or annotations
                if (content.startswith('To override') or 
                    'Moved to' in content or 
                    'Deactivated' in content or 
                    'Billing Service' in content or
                    content.startswith('last_7_days') or
                    content.startswith('last_9_months') or
                    content.startswith('last_12_months') or
                    not content):
                    new_lines.append(line)
                else:
                    # Remove the comment character #
                    idx = line.find('#')
                    uncommented = line[:idx] + line[idx+1:]
                    new_lines.append(uncommented)
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)

    namespace = {}
    exec(''.join(new_lines), namespace)
    return namespace.get('INSTANCES', {})

# ---------------------------------------------------------------------------
# CSV Parsing Helper
# ---------------------------------------------------------------------------

def find_latest_summary_csv(csv_dir: str = 'csv_files/run_summaries') -> str:
    """Finds the latest identifier summary CSV file in the directory."""
    if not os.path.exists(csv_dir):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_dir = os.path.join(script_dir, csv_dir)
        
    pattern = os.path.join(csv_dir, 'identifier_summary_*.csv')
    files = glob.glob(pattern)
    if not files:
        # Fall back to latest_identifier_summary.csv if it exists
        fallback = os.path.join(csv_dir, 'latest_identifier_summary.csv')
        if os.path.exists(fallback):
            return fallback
        raise FileNotFoundError(f"No summary CSV files found in: {csv_dir}")
    
    # Sort files by name (which contains timestamp)
    files.sort()
    return files[-1]

def get_failed_runs_from_csv(csv_path: str) -> list[dict]:
    """Reads a summary CSV file and returns rows where db_updated is 'FALSE'."""
    if not os.path.exists(csv_path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(script_dir, csv_path)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at: {csv_path}")

    failed_runs = []
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('db_updated') == 'FALSE':
                failed_runs.append(row)
    return failed_runs

# ---------------------------------------------------------------------------
# Rerun Orchestrator
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rerun failed report runs from a past summary CSV."
    )
    parser.add_argument(
        '--csv', '-c',
        type=str,
        default=None,
        help="Path to the summary CSV file to parse for failures (default: latest timestamped summary)"
    )
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=None,
        help="Number of instances to process concurrently (default: number of instances in failures)"
    )
    parser.add_argument(
        '--dry-run', '-d',
        action='store_true',
        help="Perform a dry run without calling real APIs or updating the database."
    )
    args = parser.parse_args()

    # Step 1: Bootstrap the DB config and verify we can connect
    logging.info("Bootstrapping database connection...")
    generate_identifiers._bootstrap()
    
    # Ensure tables exist
    generate_identifiers.create_account_reports_table_if_not_exists()

    # Step 2: Load the full instances config (including commented out ones)
    logging.info("Loading full instances configuration (including commented-out instances)...")
    try:
        instances_config = load_full_instances_config('config/config.py')
    except Exception as exc:
        logging.error("Failed to load instances configuration: %s", exc)
        sys.exit(1)

    # Step 3: Find and parse the failure CSV file
    csv_file = args.csv
    if not csv_file:
        try:
            csv_file = find_latest_summary_csv()
        except Exception as exc:
            logging.error("Failed to find latest summary CSV: %s", exc)
            sys.exit(1)

    logging.info("Reading failures from: %s", csv_file)
    try:
        failed_runs = get_failed_runs_from_csv(csv_file)
    except Exception as exc:
        logging.error("Failed to parse CSV file %s: %s", csv_file, exc)
        sys.exit(1)

    if not failed_runs:
        logging.info("No failed report runs found in the CSV! Everything succeeded. Exiting.")
        sys.exit(0)

    if args.dry_run:
        logging.info("=== DRY RUN MODE: No real API calls or database updates will occur ===")

    logging.info("Found %d failed run(s) to rerun.", len(failed_runs))

    # Step 4: Group failed runs by instance_key
    grouped_failures = {}
    for run in failed_runs:
        inst_key = run['instance_key']
        if inst_key not in grouped_failures:
            grouped_failures[inst_key] = []
        grouped_failures[inst_key].append(run)

    # Step 5: Execute reruns per instance
    results_list = []
    results_lock = threading.Lock()

    def process_instance_reruns(instance_key: str, runs: list) -> None:
        if instance_key not in instances_config:
            logging.error("Instance key '%s' not found in config/config.py! Cannot rerun %d reports.",
                          instance_key, len(runs))
            # Record them as failures in the rerun results
            for run in runs:
                results_list.append(generate_identifiers.AttemptRecord(
                    instance_key=instance_key,
                    customer_account=run['customer_account'],
                    customer_name=run['customer_name'],
                    report_name=run['report_name'],
                    report_id=run['report_id'],
                    filter_id=run['filter_id'],
                    http_status=0,
                    api_status="CONFIG_MISSING",
                    identifier="None",
                    status_message=f"Instance key '{instance_key}' missing from config",
                    retries=0,
                    db_updated='FALSE'
                ))
            return

        inst_cfg = instances_config[instance_key]
        base_url = inst_cfg['api_base_url']
        username = inst_cfg['username']
        password = inst_cfg['password']

        logging.info("=" * 80)
        logging.info("RERUN INSTANCE: %s (%d reports failed)", instance_key, len(runs))
        logging.info("=" * 80)

        for i, run in enumerate(runs):
            logging.info("[%s] Rerunning report %d/%d: %s for account %s (%s)",
                         instance_key, i + 1, len(runs),
                         run['report_name'], run['customer_name'], run['customer_account'])
            
            if args.dry_run:
                logging.info("[Dry Run] Would call API for report %s (ID %s, Filter %s) and account %s (%s)",
                             run['report_name'], run['report_id'], run['filter_id'],
                             run['customer_name'], run['customer_account'])
                with results_lock:
                    results_list.append(generate_identifiers.AttemptRecord(
                        instance_key=instance_key,
                        customer_account=run['customer_account'],
                        customer_name=run['customer_name'],
                        report_name=run['report_name'],
                        report_id=run['report_id'],
                        filter_id=run['filter_id'],
                        http_status=200,
                        api_status="DRY_RUN_SUCCESS",
                        identifier="MOCK_ID_" + run['report_id'],
                        status_message="Dry run simulation success",
                        retries=0,
                        db_updated='TRUE'
                    ))
            else:
                generate_identifiers.run_report_for_account(
                    account=run['customer_account'],
                    account_name=run['customer_name'],
                    report_id=run['report_id'],
                    filter_id=run['filter_id'],
                    report_name=run['report_name'].lower(),  # generate_identifiers.py lowercases internally for table queries
                    instance_key=instance_key,
                    base_url=base_url,
                    username=username,
                    password=password,
                    results_list=results_list,
                    results_lock=results_lock
                )

    instance_keys = list(grouped_failures.keys())
    if args.workers is None:
        max_workers = min(32, max(1, len(instance_keys)))
    else:
        max_workers = args.workers

    logging.info("Processing %d instance(s) concurrently using %d workers.", len(instance_keys), max_workers)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_instance_reruns, key, grouped_failures[key]): key
            for key in instance_keys
        }
        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            try:
                future.result()
            except Exception as exc:
                logging.error("Rerun processing for instance %s raised an exception: %s", key, exc)

    # Step 6: Write rerun summary CSV (skip copying to latest_identifier_summary.csv in dry-run to preserve real summary)
    logging.info("Rerun process completed. Writing summaries...")
    if args.dry_run:
        logging.info("[Dry Run] Skipping summary file writing.")
    else:
        generate_identifiers.write_runs_summary(results_list)
    
    # Calculate success/failure statistics
    success_count = sum(1 for r in results_list if r['db_updated'] == 'TRUE')
    fail_count = sum(1 for r in results_list if r['db_updated'] == 'FALSE')
    logging.info("Rerun stats — Successes: %d, Failures: %d", success_count, fail_count)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Rerun process interrupted by user.")
        sys.exit(1)
