"""
generate_identifiers.py
~~~~~~~~~~~~~~~~~~~~~~~
Submits report-run requests to the CollaborateMD API for every configured account
and instance, then records the resulting report identifier in the database.

Instances are processed in parallel (one thread per instance); accounts within
each instance are processed sequentially to stay within API rate limits.

Usage::

    python generate_identifiers.py [--workers N]

Exit codes:
    0  All reports submitted successfully.
    1  Configuration invalid, or interrupted by the user.
"""

import argparse
import concurrent.futures
import csv
import logging
import os
import sys
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, TypedDict

import psycopg2
import requests

from logging_utils import setup_file_logging
from db_utils import get_db_cursor

# ---------------------------------------------------------------------------
# Setup — must happen before any other imports that produce output
# ---------------------------------------------------------------------------
setup_file_logging("generate_identifiers.log")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_RETRIES = 100           # Maximum number of retry attempts per report
RETRY_DELAY_SECS = 60       # Seconds to wait between retries (RUNNING / DUPLICATE)
INTER_ACCOUNT_DELAY_SECS = 5  # Polite delay between consecutive account calls

API_NAMESPACE = {'ns1': 'http://www.collaboratemd.com/api/v1/'}

# ---------------------------------------------------------------------------
# Config — loaded once at startup (inside _bootstrap, not at module level)
# ---------------------------------------------------------------------------
config_loader = None   # type: ignore[assignment]
postgres_config: dict = {}
schema: str = ""


def _bootstrap() -> None:
    """
    Load and validate configuration.  Called once from ``__main__`` so that
    importing this module does not trigger config loading or sys.exit().
    """
    global config_loader, postgres_config, schema

    from config_loader import ConfigLoader

    config_loader = ConfigLoader('config/config.py')
    postgres_config = config_loader.get_postgres_config()
    schema = postgres_config['schema']

    is_valid, errors = config_loader.validate_instances()
    if not is_valid:
        for error in errors:
            logging.error("Configuration error: %s", error)
        raise SystemExit(1)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def postgres_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=postgres_config['host'],
        user=postgres_config['user'],
        password=postgres_config['password'],
        dbname=postgres_config['database'],
        port=postgres_config['port'],
    )


def create_account_reports_table_if_not_exists() -> None:
    """
    Creates the ``account_reports`` table and its index in the database
    if they do not already exist.
    """
    try:
        with get_db_cursor(postgres_connection) as (_, cur):
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.account_reports (
                    id SERIAL PRIMARY KEY,
                    customer_account VARCHAR(20) NOT NULL,
                    report_name VARCHAR(100) NOT NULL,
                    identifier VARCHAR(20) NOT NULL,
                    status INTEGER DEFAULT 1,
                    instance_key VARCHAR(100) NOT NULL
                );
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_account_report
                ON {schema}.account_reports (customer_account, report_name);
            """)
        logging.info("✓ Ensured database table '%s.account_reports' exists.", schema)
    except Exception as exc:
        logging.error("Error creating account_reports table: %s", exc)


# ---------------------------------------------------------------------------
# XML parsing
# ---------------------------------------------------------------------------

def _parse_report_xml(response_text: str) -> Optional[dict]:
    """
    Parse the API XML response and return a dict with keys:
    ``status``, ``identifier``, ``status_message``.

    Returns ``None`` and logs an error if any required element is missing
    or if the XML is malformed.
    """
    try:
        root = ET.fromstring(response_text)
    except ET.ParseError as exc:
        logging.error("Failed to parse XML response: %s", exc)
        return None

    status_elem = root.find('ns1:Status', API_NAMESPACE)
    identifier_elem = root.find('ns1:Identifier', API_NAMESPACE)
    status_message_elem = root.find('ns1:StatusMessage', API_NAMESPACE)

    if status_elem is None or identifier_elem is None or status_message_elem is None:
        logging.error(
            "Missing required XML elements in response (Status=%s, Identifier=%s, StatusMessage=%s)",
            status_elem, identifier_elem, status_message_elem,
        )
        return None

    return {
        'status': status_elem.text,
        'identifier': identifier_elem.text,
        'status_message': status_message_elem.text,
    }


# ---------------------------------------------------------------------------
# Core report-handling logic
# ---------------------------------------------------------------------------

def handle_report_response(
    response_text: str,
    customer_account: str,
    report_name: str,
    instance_key: str,
) -> tuple[str, Optional[str], str, str]:
    """
    Parse the API response XML and, on success, persist the identifier to the DB.

    Returns ``(result, identifier, api_status, status_message)`` where ``result``
    is one of: ``'SUCCESS'``, ``'RUNNING'``, ``'DUPLICATE'``, ``'ERROR'``.
    """
    ctx = f"{report_name} | account {customer_account} | instance {instance_key}"

    parsed = _parse_report_xml(response_text)
    if parsed is None:
        logging.error("Could not parse response for %s", ctx)
        return "ERROR", None, "XML_PARSE_ERROR", "Failed to parse XML"

    status = parsed['status']
    identifier = parsed['identifier']
    status_message = parsed['status_message']

    logging.info("identifier: %s | StatusMessage: %s | Instance: %s",
                 identifier, status_message, instance_key)

    # Report is still being generated — tell the caller to retry
    if "still running" in (status_message or ""):
        return "RUNNING", identifier, status, status_message

    if status not in ('SUCCESS', 'REPORT RUNNING'):
        logging.warning("Unexpected status '%s' for %s", status, ctx)
        return "ERROR", None, status, status_message

    # Persist the identifier, guarding against duplicates
    try:
        with get_db_cursor(postgres_connection) as (_, cur):
            cur.execute(
                f"SELECT 1 FROM {schema}.account_reports "
                f"WHERE customer_account = %s AND report_name = %s "
                f"AND identifier = %s AND instance_key = %s",
                (customer_account, report_name, identifier, instance_key),
            )
            if cur.fetchone():
                logging.info(
                    "Duplicate identifier %s for %s", identifier, ctx
                )
                return "DUPLICATE", identifier, status, status_message

            # Deactivate previous identifiers for this account/report/instance
            cur.execute(
                f"UPDATE {schema}.account_reports SET status = 0 "
                f"WHERE customer_account = %s AND report_name = %s "
                f"AND status = 1 AND instance_key = %s",
                (customer_account, report_name, instance_key),
            )
            cur.execute(
                f"INSERT INTO {schema}.account_reports "
                f"(customer_account, report_name, identifier, status, instance_key) "
                f"VALUES (%s, %s, %s, 1, %s)",
                (customer_account, report_name, identifier, instance_key),
            )
        return "SUCCESS", identifier, status, status_message

    except psycopg2.Error as exc:
        logging.error("Database error for %s: %s", ctx, exc)
        return "ERROR", None, "DB_ERROR", str(exc)


# ---------------------------------------------------------------------------
# Result recording
# ---------------------------------------------------------------------------

class AttemptRecord(TypedDict):
    instance_key: str
    customer_account: str
    customer_name: str
    report_name: str
    report_id: str
    filter_id: str
    http_status: int
    api_status: str
    identifier: str
    status_message: str
    retries: int
    db_updated: str  # 'TRUE' | 'FALSE'


def record_attempt(
    record: AttemptRecord,
    results_list: list,
    results_lock: threading.Lock,
) -> None:
    """Append an attempt record to the shared results list, thread-safely."""
    with results_lock:
        results_list.append(record)


# ---------------------------------------------------------------------------
# Per-account report submission
# ---------------------------------------------------------------------------

def run_report_for_account(
    account: str,
    account_name: str,
    report_id: str,
    filter_id: str,
    report_name: str,
    instance_key: str,
    base_url: str,
    username: str,
    password: str,
    results_list: list,
    results_lock: threading.Lock,
) -> None:
    """
    Submit a report-run request for a single customer account.

    Retries up to ``MAX_RETRIES`` times when the report is still running or
    the API returns a duplicate identifier.  Records the outcome (including
    any failure) in ``results_list``.
    """
    url = f"{base_url}/customer/{account}/reports/{report_id}/filter/{filter_id}/run"
    headers = {"Content-Type": "application/xml"}
    ctx = f"{report_name.upper()} | account {account_name} ({account}) | instance {instance_key}"

    db_updated = False
    identifier: Optional[str] = None
    api_status = "UNKNOWN"
    status_message = "No response"
    http_status = 0

    for attempt in range(MAX_RETRIES):
        payload = f"<Run><Nonce>{time.time()}</Nonce></Run>"

        try:
            response = requests.post(url, data=payload, headers=headers, auth=(username, password))
            http_status = response.status_code
            logging.info("%s | HTTP %s", ctx, response.status_code)
        except Exception as exc:
            logging.error("Request exception for %s: %s", ctx, exc)
            record_attempt(
                AttemptRecord(
                    instance_key=instance_key, customer_account=account,
                    customer_name=account_name, report_name=report_name.upper(),
                    report_id=report_id, filter_id=filter_id, http_status=0,
                    api_status="EXCEPTION", identifier="None",
                    status_message=str(exc), retries=attempt, db_updated='FALSE',
                ),
                results_list, results_lock,
            )
            break

        if response.status_code != 200:
            logging.error("API call failed for %s — HTTP %s", ctx, response.status_code)
            record_attempt(
                AttemptRecord(
                    instance_key=instance_key, customer_account=account,
                    customer_name=account_name, report_name=report_name.upper(),
                    report_id=report_id, filter_id=filter_id,
                    http_status=response.status_code,
                    api_status=f"HTTP_{response.status_code}", identifier="None",
                    status_message=f"API Call Failed: HTTP {response.status_code}",
                    retries=attempt, db_updated='FALSE',
                ),
                results_list, results_lock,
            )
            break

        result, identifier, api_status, status_message = handle_report_response(
            response.text, account, report_name, instance_key
        )

        if result == "SUCCESS":
            logging.info("Report started and DB updated for %s", ctx)
            db_updated = True
            record_attempt(
                AttemptRecord(
                    instance_key=instance_key, customer_account=account,
                    customer_name=account_name, report_name=report_name.upper(),
                    report_id=report_id, filter_id=filter_id,
                    http_status=http_status, api_status=api_status,
                    identifier=str(identifier) if identifier is not None else "None",
                    status_message=status_message, retries=attempt, db_updated='TRUE',
                ),
                results_list, results_lock,
            )
            break

        elif result in ("RUNNING", "DUPLICATE"):
            reason = "still running" if result == "RUNNING" else f"duplicate identifier {identifier}"
            logging.info(
                "Report for %s is %s. Waiting %ds (attempt %d/%d)...",
                ctx, reason, RETRY_DELAY_SECS, attempt + 1, MAX_RETRIES,
            )
            time.sleep(RETRY_DELAY_SECS)
            # continue to next attempt

        else:  # ERROR or unrecognised result
            logging.error("Error handling response for %s — skipping", ctx)
            record_attempt(
                AttemptRecord(
                    instance_key=instance_key, customer_account=account,
                    customer_name=account_name, report_name=report_name.upper(),
                    report_id=report_id, filter_id=filter_id,
                    http_status=http_status, api_status=api_status,
                    identifier=str(identifier) if identifier is not None else "None",
                    status_message=status_message, retries=attempt, db_updated='FALSE',
                ),
                results_list, results_lock,
            )
            break

    else:
        # Loop exhausted without a break — max retries reached
        logging.error("Max retries (%d) reached for %s", MAX_RETRIES, ctx)
        record_attempt(
            AttemptRecord(
                instance_key=instance_key, customer_account=account,
                customer_name=account_name, report_name=report_name.upper(),
                report_id=report_id, filter_id=filter_id,
                http_status=http_status, api_status="MAX_RETRIES",
                identifier="None",
                status_message=f"Max retries ({MAX_RETRIES}) reached",
                retries=MAX_RETRIES, db_updated='FALSE',
            ),
            results_list, results_lock,
        )

    time.sleep(INTER_ACCOUNT_DELAY_SECS)


# ---------------------------------------------------------------------------
# Per-instance orchestration
# ---------------------------------------------------------------------------

def generate_report_for_all_accounts(
    report_id: str,
    filter_id: str,
    report_name: str,
    instance_key: str,
    base_url: str,
    username: str,
    password: str,
    accounts: list,
    account_names: dict,
    results_list: list,
    results_lock: threading.Lock,
    account_filters: Optional[list] = None,
) -> None:
    """
    Submit the given report for every account in ``accounts``.

    ``account_filters`` is an optional list of ``{"account": ..., "filter_id": ...}``
    dicts that override the default ``filter_id`` for specific accounts.
    """
    # Build an O(1) lookup for per-account filter overrides
    filters_lookup = (
        {f["account"]: f["filter_id"] for f in account_filters}
        if account_filters
        else {}
    )

    for account in accounts:
        run_report_for_account(
            account=account,
            account_name=account_names.get(account, account),
            report_id=report_id,
            filter_id=filters_lookup.get(account, filter_id),
            report_name=report_name,
            instance_key=instance_key,
            base_url=base_url,
            username=username,
            password=password,
            results_list=results_list,
            results_lock=results_lock,
        )


# ---------------------------------------------------------------------------
# Summary CSV
# ---------------------------------------------------------------------------

_SUMMARY_FIELDS = [
    'instance_key', 'customer_account', 'customer_name', 'report_name', 'report_id',
    'filter_id', 'http_status', 'api_status', 'identifier', 'status_message', 'retries', 'db_updated',
]


def _write_csv(path: str, fields: list, rows: list) -> None:
    """Write a list of dicts to a CSV file with the given field order."""
    with open(path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_runs_summary(results_list: list) -> None:
    """
    Write run results to a timestamped CSV and overwrite the ``latest_*``
    convenience file.
    """
    if not results_list:
        return

    csv_dir = 'csv_files/run_summaries'
    os.makedirs(csv_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file = os.path.join(csv_dir, f"identifier_summary_{timestamp}.csv")
    latest_csv_file = os.path.join(csv_dir, "latest_identifier_summary.csv")

    try:
        _write_csv(csv_file, _SUMMARY_FIELDS, results_list)
        logging.info("✓ Runs summary written to: %s", csv_file)
    except Exception as exc:
        logging.error("✗ Failed to write run summary CSV: %s", exc)

    try:
        _write_csv(latest_csv_file, _SUMMARY_FIELDS, results_list)
        logging.info("✓ Copied latest runs summary to: %s", latest_csv_file)
    except Exception as exc:
        logging.error("✗ Failed to write latest summary CSV: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_all_reports(max_workers: Optional[int] = None) -> None:
    """
    Generate reports for all configured instances in parallel.

    Each instance runs in its own thread; accounts within an instance are
    processed sequentially to stay within API rate limits.
    """
    create_account_reports_table_if_not_exists()

    instances = config_loader.get_instances()
    instance_list = config_loader.list_instances()

    logging.info("=" * 80)
    logging.info("GENERATE IDENTIFIERS — MULTI-INSTANCE MODE")
    logging.info("=" * 80)
    logging.info("Processing %d instance(s): %s", len(instance_list), ', '.join(instance_list))

    if max_workers is None:
        max_workers = min(32, max(1, len(instance_list)))

    results_list: list = []
    results_lock = threading.Lock()

    def process_instance(instance_key: str) -> None:
        instance_config = instances[instance_key]
        logging.info("=" * 80)
        logging.info("INSTANCE: %s", instance_key)
        logging.info("API URL: %s", instance_config['api_base_url'])
        logging.info("Accounts: %s", instance_config['accounts'])
        logging.info("=" * 80)

        for report_config in config_loader.get_report_configs(instance_key=instance_key):
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
                results_lock=results_lock,
                account_filters=report_config.get("account_filters"),
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_instance = {
            executor.submit(process_instance, key): key
            for key in instance_list
        }
        for future in concurrent.futures.as_completed(future_to_instance):
            key = future_to_instance[future]
            try:
                future.result()
            except Exception as exc:
                logging.error("Instance %s raised an exception: %s", key, exc)

    write_runs_summary(results_list)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Submit report-run requests for all configured instances concurrently.'
    )
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=None,
        help='Number of instances to process concurrently (default: number of instances, up to 32)',
    )
    args = parser.parse_args()

    _bootstrap()

    try:
        run_all_reports(max_workers=args.workers)
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
        sys.exit(1)
