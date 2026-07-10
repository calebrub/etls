"""
fetch_and_load_reports.py
~~~~~~~~~~~~~~~~~~~~~~~~~
ETL pipeline: fetch report data from the CollaborateMD API, write CSVs, then
bulk-load those CSVs into PostgreSQL with incremental (hash-based) deduplication.

Pipeline stages
---------------
1. **Fetch** – Download each report for every account/instance in parallel and
   write the result to a CSV file under ``csv_files/``.
2. **Validate** – Load every CSV into a DataFrame, infer types, and verify that
   the inferred schema matches the existing DB schema (fail-fast before touching
   the database).
3. **Load** – Bulk-COPY each table into PostgreSQL using a staging-table approach
   for incremental runs, or TRUNCATE + COPY for full-refresh runs.
4. **SQL** – Execute any ``.sql`` files in the ``sql/`` folder.

Usage::

    python fetch_and_load_reports.py [--full-refresh] [--workers N]
"""

import argparse
import csv
import fcntl
import gc
import glob
import hashlib
import io
import logging
import os
import re
import sys
import threading
import time
import xml.etree.ElementTree as ET
import zipfile
import base64
import concurrent.futures
from datetime import date, datetime
from typing import Optional

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import BigInteger, Date, DateTime, Float, Text

from logging_utils import setup_file_logging

# ---------------------------------------------------------------------------
# Setup — must happen before any other imports that produce output
# ---------------------------------------------------------------------------
setup_file_logging("fetch_and_load_reports.log")

# ---------------------------------------------------------------------------
# Config — populated by _bootstrap(), not at module level
# ---------------------------------------------------------------------------
config_loader = None   # type: ignore[assignment]
postgres_config: dict = {}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Column names added synthetically during the ETL — must not appear in source CSVs
RESERVED_COLUMNS = {'created_at', 'row_hash'}

# Number of CSV rows processed per chunk in the streaming incremental loader.
# Caps peak memory so large tables (e.g. multi-million-row user_time_spread) load
# on memory-constrained hosts without being OOM-killed. Override via env if needed.
CHUNK_SIZE = int(os.environ.get('ETL_LOAD_CHUNK_SIZE', '250000'))

# libpq connection options shared by every DB connection this ETL opens.
# TCP keepalives stop the network/gateway from silently dropping a connection during
# a long-running statement — the cause of the recurring "SSL connection has been
# closed unexpectedly" failures (and of runs that hung forever waiting on a dead
# socket). statement_timeout is a backstop so a stuck statement fails loudly instead
# of hanging the process. Tunable via env for unusually long maintenance statements.
DB_CONNECT_ARGS = {
    'connect_timeout': 30,
    'keepalives': 1,
    'keepalives_idle': 30,
    'keepalives_interval': 10,
    'keepalives_count': 5,
    'options': f"-c statement_timeout={os.environ.get('ETL_STATEMENT_TIMEOUT_MS', '3600000')}",
}

# Single-run lock: prevents overlapping ETL runs (e.g. a scheduler firing a new run
# while a previous one is still going) from competing for memory and DB connections.
LOCK_FILE = os.environ.get('ETL_LOCK_FILE', '/tmp/fetch_and_load_reports.lock')


def acquire_run_lock():
    """
    Take an exclusive, non-blocking lock so only one ETL run executes at a time.

    Returns the open file handle (which must stay referenced for the process
    lifetime to hold the lock). Exits the process cleanly if another run holds it.
    """
    lock_fh = open(LOCK_FILE, 'w')
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        logging.error(
            "Another fetch_and_load_reports run is already in progress "
            "(lock held on %s). Exiting to avoid overlapping runs.", LOCK_FILE
        )
        sys.exit(0)
    lock_fh.write(str(os.getpid()))
    lock_fh.flush()
    return lock_fh


def _bootstrap() -> None:
    """
    Load configuration.  Called once from ``__main__`` so that importing this
    module does not trigger config I/O or sys.exit().
    """
    global config_loader, postgres_config

    from config_loader import ConfigLoader

    config_loader = ConfigLoader('config/config.py')
    postgres_config = config_loader.get_postgres_config()


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
        **DB_CONNECT_ARGS,
    )


def is_transient_db_error(exc: Exception) -> bool:
    """
    Check if the exception is a transient database/connection error that should be retried.
    Excludes permanent/programming/data validation issues (like syntax errors, integrity
    constraint violations, or database/schema mismatch errors).
    """
    # If it is not a database-related exception, it's a python bug (KeyError, NameError, etc.)
    if not isinstance(exc, (psycopg2.Error, SQLAlchemyError)):
        return False

    # Check class name for known non-transient types
    exc_class_name = exc.__class__.__name__
    non_transient_classes = ["ProgrammingError", "DataError", "CompileError", "ArgumentError", "IntegrityError"]
    if any(term in exc_class_name for term in non_transient_classes):
        return False

    # Check Postgres error code (pgcode) if available
    pgcode = None
    if hasattr(exc, 'orig') and hasattr(exc.orig, 'pgcode') and exc.orig.pgcode:
        pgcode = exc.orig.pgcode
    elif hasattr(exc, 'pgcode') and exc.pgcode:
        pgcode = exc.pgcode

    if pgcode:
        # Class 42: Syntax Error or Access Rule Violation (e.g. undefined_table, undefined_column)
        # Class 22: Data Exception (e.g. numeric_value_out_of_range, string_data_right_truncation)
        # Class 23: Integrity Constraint Violation (e.g. unique_violation, foreign_key_violation)
        if pgcode.startswith('42') or pgcode.startswith('22') or pgcode.startswith('23'):
            return False

    return True


def load_report_matrix(instance_key: Optional[str] = None,
                        has_instance_column: bool = True) -> dict:
    """
    Load active report identifiers from the database.

    Args:
        instance_key: If provided (and the column exists), only return rows
                      for this instance.
        has_instance_column: Whether the ``instance_key`` column exists in the
                             table.  Pass ``False`` to skip the per-instance
                             filter without an extra round-trip to check.

    Returns:
        ``{customer_account: {report_name: identifier}}``
    """
    conn = postgres_connection()
    cursor = conn.cursor()
    schema = postgres_config['schema']

    if has_instance_column and instance_key:
        cursor.execute(
            f"SELECT customer_account, report_name, identifier "
            f"FROM {schema}.account_reports WHERE status = 1 AND instance_key = %s",
            (instance_key,),
        )
    else:
        cursor.execute(
            f"SELECT customer_account, report_name, identifier "
            f"FROM {schema}.account_reports WHERE status = 1"
        )

    report_matrix: dict = {}
    for customer_account, report_name, identifier in cursor.fetchall():
        report_matrix.setdefault(customer_account, {})[report_name] = identifier

    cursor.close()
    conn.close()
    return report_matrix


def has_instance_key_column(schema: str) -> bool:
    """Return ``True`` if the ``account_reports`` table has an ``instance_key`` column."""
    conn = postgres_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = 'account_reports' AND column_name = 'instance_key'
        """,
        (schema,),
    )
    has_column = cursor.fetchone() is not None
    cursor.close()
    conn.close()
    return has_column


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def find_element(root: ET.Element, local_name: str) -> Optional[ET.Element]:
    """
    Find the first descendant element matching ``local_name``, ignoring
    XML namespaces.
    """
    for elem in root.iter():
        if elem.tag.split('}')[-1] == local_name:
            return elem
    return None


# ---------------------------------------------------------------------------
# Fetch phase
# ---------------------------------------------------------------------------

def fetch_report_data_for_customer(
    base_url: str,
    username: str,
    password: str,
    customer_id: str,
    customer_name: str,
    report_id: str,
    report_name: str,
    instance_key: str,
    max_retries: int = 100,
    retry_delay: int = 60,
) -> tuple:
    """
    Fetch, decode, and parse a single report result from the CollaborateMD API.

    Returns:
        ``(headers, rows, http_status, api_status, status_message, retries)``

        ``headers`` and ``rows`` are ``None`` when no data was returned.
        Each row is prefixed with ``[customer_id, instance_key]``.
    """
    http_status = 0
    api_status = "UNKNOWN"
    status_message = "No response"

    for attempt in range(max_retries):
        url = f"{base_url}/customer/{customer_id}/reports/results/{report_id}"
        try:
            response = requests.post(url, auth=(username, password))
            http_status = response.status_code
        except requests.exceptions.RequestException as exc:
            api_status = "EXCEPTION"
            status_message = str(exc)
            logging.error(
                "  → Request failed for %s — account %s (%s): %s",
                report_name, customer_name, customer_id, exc,
            )
            return None, None, http_status, api_status, status_message, attempt

        if response.status_code != 200:
            api_status = f"HTTP_{response.status_code}"
            status_message = f"API call failed with status code {response.status_code}"
            logging.error(
                "  → HTTP %s for %s — account %s (%s)",
                response.status_code, report_name, customer_name, customer_id,
            )
            return None, None, http_status, api_status, status_message, attempt

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as exc:
            api_status = "XML_PARSE_ERROR"
            status_message = f"Failed to parse XML response: {exc}"
            logging.error(
                "  → XML parse error for %s — account %s (%s): %s",
                report_name, customer_name, customer_id, exc,
            )
            return None, None, http_status, api_status, status_message, attempt

        data_element = find_element(root, 'Data')
        status_element = find_element(root, 'Status')
        status_msg_element = find_element(root, 'StatusMessage')

        if status_msg_element is not None:
            status_message = status_msg_element.text or ""

        if status_element is not None:
            api_status = status_element.text or ""

        # Report still running — wait and retry
        if status_element is not None and status_element.text == 'REPORT RUNNING':
            logging.info(
                "  → [RUNNING] %s for account %s (%s). Waiting %ds... (attempt %d/%d)",
                report_name, customer_name, customer_id, retry_delay, attempt + 1, max_retries,
            )
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            else:
                api_status = "TIMEOUT"
                logging.warning(
                    "  → [TIMEOUT] %s for account %s (%s) did not complete in time.",
                    report_name, customer_name, customer_id,
                )
                return None, None, http_status, api_status, status_message, attempt

        if data_element is not None and data_element.text:
            try:
                zip_bytes = base64.b64decode(data_element.text)
                with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
                    for zip_info in zip_file.infolist():
                        if zip_info.filename.endswith('.csv'):
                            with zip_file.open(zip_info) as csv_file:
                                decoded = io.TextIOWrapper(csv_file, encoding='utf-8')
                                csv_reader = csv.reader(decoded)
                                try:
                                    headers = [h.strip() for h in next(csv_reader)]
                                except StopIteration:
                                    continue

                                rows = []
                                for row in csv_reader:
                                    row_values = [v.strip() if v.strip() else None for v in row]
                                    while len(row_values) < len(headers):
                                        row_values.append(None)
                                    rows.append([customer_id, instance_key] + row_values)

                                api_status = "SUCCESS"
                                return headers, rows, http_status, api_status, status_message, attempt
            except Exception as exc:
                api_status = "ZIP_CSV_ERROR"
                status_message = f"Failed to process zip/CSV: {exc}"
                logging.error(
                    "  → Failed to process zip/CSV for %s — account %s (%s): %s",
                    report_name, customer_name, customer_id, exc,
                )
                return None, None, http_status, api_status, status_message, attempt
        else:
            xml_status = api_status
            api_status = "NO_DATA"
            api_msg = (
                f" — API Message: {status_message}"
                if status_message and status_message != "No response"
                else ""
            )
            logging.warning(
                "  → No data element for %s — account %s (%s) (HTTP %s, XML Status: %s)%s",
                report_name, customer_name, customer_id, http_status, xml_status, api_msg,
            )
            return None, None, http_status, api_status, status_message, attempt

    return None, None, http_status, api_status, status_message, max_retries - 1


# ---------------------------------------------------------------------------
# Thread-safe fetch session (replaces module-level mutable globals)
# ---------------------------------------------------------------------------

class FetchSession:
    """
    Encapsulates all mutable, per-run state used by the parallel fetch workers.

    Creating a new ``FetchSession`` per call to ``fetch_reports_to_csv`` means
    there is no manual state-clearing between runs and no risk of stale data
    leaking from a previous execution.
    """

    def __init__(self) -> None:
        self._csv_write_locks: dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()

        self.initialized_files: set[str] = set()
        self._init_lock = threading.Lock()

        self.written_reports: set[tuple] = set()
        self._reports_lock = threading.Lock()

    def get_csv_lock(self, file_path: str) -> threading.Lock:
        """Return a per-file write lock, creating it lazily."""
        with self._locks_lock:
            if file_path not in self._csv_write_locks:
                self._csv_write_locks[file_path] = threading.Lock()
            return self._csv_write_locks[file_path]

    def write_report_to_csv(
        self,
        csv_dir: str,
        instance_key: str,
        report_name: str,
        headers: list,
        rows: list,
        instance_count: int,
    ) -> None:
        """
        Write (or append) report rows to a CSV file, thread-safely.

        When more than one instance exists, rows are written under an
        instance-specific subdirectory so that accounts from different
        instances never share a file.
        """
        if instance_count > 1:
            instance_dir = os.path.join(csv_dir, instance_key)
            os.makedirs(instance_dir, exist_ok=True)
            file_path = os.path.join(instance_dir, f"{report_name}.csv")
        else:
            file_path = os.path.join(csv_dir, f"{report_name}.csv")

        lock = self.get_csv_lock(file_path)
        with lock:
            with self._init_lock:
                already_initialized = file_path in self.initialized_files
                if not already_initialized:
                    self.initialized_files.add(file_path)

            mode = 'a' if already_initialized else 'w'
            with open(file_path, mode, newline='', encoding='utf-8') as fh:
                writer = csv.writer(fh)
                if not already_initialized:
                    writer.writerow(headers)
                writer.writerows(rows)

        rows_with_account = sum(1 for r in rows if r[0])
        logging.info(
            "✓ Fetched %s: %d rows (%d with account) for instance %s",
            report_name, len(rows), rows_with_account, instance_key,
        )

    def mark_written(self, instance_key: str, report_name: str) -> None:
        with self._reports_lock:
            self.written_reports.add((instance_key, report_name.upper()))

    def was_written(self, instance_key: str, report_name: str) -> bool:
        with self._reports_lock:
            return (instance_key, report_name.upper()) in self.written_reports


# ---------------------------------------------------------------------------
# Task collection
# ---------------------------------------------------------------------------

def collect_fetch_tasks(
    instance_list: list,
    instances: dict,
    has_instance_column: bool,
) -> list:
    """
    Build the flat list of ``(instance, account, report)`` fetch tasks.

    Iterates every instance and every customer account to produce one task dict
    per ``(account, report)`` combination that has a valid identifier in the DB.
    """
    tasks = []
    for instance_key in instance_list:
        instance_config = instances[instance_key]
        customers = instance_config['accounts']
        account_names = instance_config.get('account_names', {})

        report_matrix = load_report_matrix(
            instance_key=instance_key,
            has_instance_column=has_instance_column,
        )

        if not report_matrix:
            logging.warning("No reports found for instance %s", instance_key)
            continue

        # Collect every report name seen across all accounts for this instance
        all_report_names = set()
        for customer in customers:
            all_report_names.update(report_matrix.get(customer, {}).keys())

        for report_name in sorted(all_report_names):
            for customer_id in customers:
                report_id = report_matrix.get(customer_id, {}).get(report_name)
                if not report_id:
                    continue
                tasks.append({
                    'instance_key': instance_key,
                    'customer_id': customer_id,
                    'customer_name': account_names.get(customer_id, customer_id),
                    'report_name': report_name,
                    'report_id': report_id,
                    'base_url': instance_config['api_base_url'],
                    'username': instance_config['username'],
                    'password': instance_config['password'],
                })
    return tasks


# ---------------------------------------------------------------------------
# Summary CSV
# ---------------------------------------------------------------------------

_FETCH_SUMMARY_FIELDS = [
    'instance_key', 'customer_account', 'customer_name', 'report_name', 'report_id',
    'http_status', 'api_status', 'status_message', 'retries', 'rows_fetched', 'file_written',
    'load_status', 'rows_inserted', 'rows_duplicate', 'load_error',
]


def _write_csv(path: str, fields: list, rows: list) -> None:
    """Write a list of dicts to a CSV file with the given field order."""
    with open(path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_fetch_summary(results_list: list) -> None:
    """
    Write fetch + load metadata to a timestamped CSV and overwrite the
    ``latest_*`` convenience file.
    """
    if not results_list:
        return

    summary_dir = 'csv_files/fetch_summaries'
    os.makedirs(summary_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file = os.path.join(summary_dir, f"fetch_summary_{timestamp}.csv")
    latest_csv_file = os.path.join(summary_dir, "latest_fetch_summary.csv")

    # Fill defaults for rows that never reached the load phase
    for row in results_list:
        row.setdefault('load_status', 'SKIPPED' if row.get('file_written') == 'FALSE' else 'PENDING')
        row.setdefault('rows_inserted', 0)
        row.setdefault('rows_duplicate', 0)
        row.setdefault('load_error', '')

    try:
        _write_csv(csv_file, _FETCH_SUMMARY_FIELDS, results_list)
        logging.info("✓ Fetch summary written to: %s", csv_file)
    except Exception as exc:
        logging.error("✗ Failed to write fetch summary CSV: %s", exc)

    try:
        _write_csv(latest_csv_file, _FETCH_SUMMARY_FIELDS, results_list)
        logging.info("✓ Copied latest fetch summary to: %s", latest_csv_file)
    except Exception as exc:
        logging.error("✗ Failed to write latest summary CSV: %s", exc)


# ---------------------------------------------------------------------------
# Fetch orchestration
# ---------------------------------------------------------------------------

def fetch_reports_to_csv(max_workers: int = 8) -> list:
    """
    Fetch reports for all instances in parallel and write them to CSV files.

    Returns the ``results_list`` (one entry per report/account) so it can be
    enriched with load-phase statistics and written to the fetch summary.
    """
    instances = config_loader.get_instances()
    instance_list = config_loader.list_instances()
    schema = postgres_config['schema']

    logging.info("=" * 80)
    logging.info("FETCH REPORTS TO CSV — MULTI-INSTANCE MODE")
    logging.info("=" * 80)
    logging.info("Processing %d instance(s): %s", len(instance_list), ', '.join(instance_list))

    # Check once whether the instance_key column exists — avoids N round-trips
    has_instance_column = has_instance_key_column(schema)

    csv_dir = 'csv_files'
    os.makedirs(csv_dir, exist_ok=True)

    tasks = collect_fetch_tasks(instance_list, instances, has_instance_column)
    if not tasks:
        logging.info("No tasks to execute.")
        return []

    # One FetchSession per run — no manual state clearing required
    session = FetchSession()

    results_list: list = []
    results_lock = threading.Lock()

    def worker(task: dict) -> None:
        instance_key = task['instance_key']
        customer_id = task['customer_id']
        customer_name = task['customer_name']
        report_name = task['report_name']
        report_id = task['report_id']

        headers, rows, http_status, api_status, status_message, retries = (
            fetch_report_data_for_customer(
                base_url=task['base_url'],
                username=task['username'],
                password=task['password'],
                customer_id=customer_id,
                customer_name=customer_name,
                report_id=report_id,
                report_name=report_name,
                instance_key=instance_key,
            )
        )

        rows_fetched = len(rows) if rows else 0

        with results_lock:
            results_list.append({
                'instance_key': instance_key,
                'customer_account': customer_id,
                'customer_name': customer_name,
                'report_name': report_name.upper(),
                'report_id': report_id,
                'http_status': http_status,
                'api_status': api_status,
                'status_message': status_message,
                'retries': retries,
                'rows_fetched': rows_fetched,
            })

        if rows:
            full_headers = ['customer_account', 'instance_key'] + (headers or [])
            session.write_report_to_csv(
                csv_dir, instance_key, report_name, full_headers, rows, len(instance_list)
            )
            session.mark_written(instance_key, report_name)

    effective_workers = min(max_workers, max(1, len(tasks)))
    logging.info(
        "Fetching %d reports in parallel using %d threads...", len(tasks), effective_workers
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as executor:
        executor.map(worker, tasks)

    # Annotate each result with whether its CSV file was actually written
    for result in results_list:
        result['file_written'] = (
            'TRUE' if session.was_written(result['instance_key'], result['report_name'])
            else 'FALSE'
        )

    return results_list


# ---------------------------------------------------------------------------
# Type inference and coercion helpers
# ---------------------------------------------------------------------------

def to_snake_case(name: str) -> str:
    """Convert an arbitrary string to a lowercase snake_case identifier."""
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.lower().replace(' ', '_')
    name = re.sub(r'_+', '_', name)
    return name.strip('_') or "unnamed_column"


def safe_column_name(name: str) -> str:
    """
    Convert to snake_case and prefix with ``src_`` if the result would collide
    with a synthetic ETL column (``created_at``, ``row_hash``).
    """
    snake = to_snake_case(name)
    return f"src_{snake}" if snake in RESERVED_COLUMNS else snake


_db_structure_cache: dict = {}
_db_structure_cache_lock = threading.Lock()


def get_db_structure(engine, schema: str, table_name: str) -> Optional[list]:
    """
    Return a list of ``(column_name, data_type)`` tuples for ``table_name``.
    Returns ``None`` if the table does not exist.

    Results are cached in-process so repeated calls within one ETL run do not
    hit the database.
    """
    cache_key = (schema, table_name)
    with _db_structure_cache_lock:
        if cache_key in _db_structure_cache:
            return _db_structure_cache[cache_key]

    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
        ORDER BY ordinal_position
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(query), {"schema": schema, "table": table_name}
        ).fetchall()

    result = [(r.column_name, r.data_type) for r in rows] or None

    with _db_structure_cache_lock:
        _db_structure_cache[cache_key] = result

    return result


def promote_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Attempt to promote text columns to numeric types where safe to do so.

    A column is **not** promoted if any non-empty value has a leading zero
    followed by another digit (e.g. ``"0301"``).  Such values are
    identifiers/codes and must stay as text to avoid silent data loss.
    """
    for col in df.columns:
        if df[col].dtype != 'object':
            continue
        if 'date' in col.lower():
            continue  # handled by promote_date_columns

        numeric_series = pd.to_numeric(df[col], errors='coerce')
        non_null_original = df[col].notna() & (df[col] != '')
        non_null_converted = numeric_series.notna()

        # Only promote when *every* non-empty value converted successfully
        if not non_null_original.any():
            continue
        if not non_null_converted[non_null_original].all():
            continue

        # Guard: leading-zero values are codes, not numbers
        has_leading_zero = (
            df[col][non_null_original]
            .astype(str)
            .str.strip()
            .str.match(r'^0\d')
            .any()
        )
        if has_leading_zero:
            continue

        # Float64 with only whole-number values → use Pandas nullable Int64
        if numeric_series.dtype == 'float64':
            non_null_vals = numeric_series.dropna()
            if len(non_null_vals) > 0 and (non_null_vals % 1 == 0).all():
                numeric_series = numeric_series.astype('Int64')

        df[col] = numeric_series

    return df


def promote_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Promote a column to ``date`` if:

    1. The column name contains ``'date'``, **or**
    2. All non-null/non-empty values match ``mm/dd/yyyy`` format.
    """
    date_pattern = re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$')

    for col in df.columns:
        cleaned = df[col].replace("", pd.NA)
        if cleaned.isna().all():
            continue

        parsed = pd.to_datetime(cleaned, format="%m/%d/%Y", errors="coerce")
        non_null_mask = cleaned.notna()

        # Skip if any non-null value failed to parse
        if (non_null_mask & parsed.isna()).any():
            continue

        has_date_in_name = "date" in col.lower()
        sample_values = cleaned.dropna().head(10)
        looks_like_date = (
            len(sample_values) > 0
            and sample_values.astype(str).apply(lambda x: bool(date_pattern.match(x))).any()
        )

        if has_date_in_name or looks_like_date:
            df[col] = parsed.dt.date
            logging.info("  → Converted '%s' to date type", col)

    return df


def infer_df_structure(df: pd.DataFrame) -> list:
    """
    Infer the expected PostgreSQL column types from DataFrame dtypes.

    Rules (in priority order):
    1. ``created_at`` → ``timestamp without time zone``
    2. ``datetime64`` dtype → ``timestamp without time zone``
    3. ``int64`` / ``Int64`` → ``bigint``
    4. ``float64`` → ``double precision``
    5. Python ``date`` values (not ``datetime``) → ``date``
    6. Everything else → ``text``
    """
    structure = []
    for col in df.columns:
        dtype = df[col].dtype

        if col.lower() == 'created_at':
            structure.append((col, "timestamp without time zone"))
            continue

        if pd.api.types.is_datetime64_any_dtype(dtype):
            structure.append((col, "timestamp without time zone"))
            continue

        if dtype in ('int64', 'Int64'):
            structure.append((col, "bigint"))
            continue

        if dtype == 'float64':
            structure.append((col, "double precision"))
            continue

        # Detect Python date objects (exclude datetime subclass)
        is_date_only = False
        non_null = df[col].dropna()
        if len(non_null) > 0:
            sample = non_null.head(100)
            try:
                sample_ok = all(isinstance(v, date) and not isinstance(v, datetime) for v in sample)
                if sample_ok:
                    is_date_only = (
                        True
                        if len(non_null) <= 100
                        else non_null.apply(
                            lambda v: isinstance(v, date) and not isinstance(v, datetime)
                        ).all()
                    )
            except Exception:
                is_date_only = False

        structure.append((col, "date" if is_date_only else "text"))

    return structure


def get_sqlalchemy_dtypes(df: pd.DataFrame) -> dict:
    """
    Map inferred DB column types to their SQLAlchemy equivalents for use with
    ``DataFrame.to_sql(dtype=...)``.
    """
    type_map = {
        "timestamp without time zone": DateTime(),
        "bigint": BigInteger(),
        "double precision": Float(),
        "date": Date(),
    }
    return {
        col: type_map.get(db_type, Text())
        for col, db_type in infer_df_structure(df)
    }


# ---------------------------------------------------------------------------
# SQL execution
# ---------------------------------------------------------------------------

def run_sql_files(engine, schema: str, sql_folder: str = 'sql') -> None:
    """Execute all ``.sql`` files in ``sql_folder`` in alphabetical order."""
    logging.info("\n" + "=" * 80)
    logging.info("RUNNING SQL FILES")
    logging.info("=" * 80)

    sql_files = sorted(glob.glob(os.path.join(sql_folder, '*.sql')))
    if not sql_files:
        logging.warning("⚠ No SQL files found in '%s' folder", sql_folder)
        return

    failed_files = []

    for sql_file in sql_files:
        filename = os.path.basename(sql_file)
        logging.info("Executing: %s", filename)

        max_retries = 3
        backoff_factors = [10, 30, 90]

        for attempt in range(1, max_retries + 1):
            try:
                with open(sql_file, 'r') as fh:
                    sql = fh.read()
                # Prepend search_path and disable statement timeout for this execution
                sql = f"SET search_path TO {schema};\nSET statement_timeout = 0;\n" + sql
                with engine.begin() as conn:
                    conn.execute(text(sql))
                logging.info("✓ Successfully executed: %s", filename)
                break
            except Exception as exc:
                is_trans = is_transient_db_error(exc)

                try:
                    engine.dispose()
                except Exception as disp_err:
                    logging.debug("Error disposing engine on SQL retry: %s", disp_err)

                if not is_trans or attempt == max_retries:
                    logging.error("✗ Failed to execute %s: %s", filename, exc)
                    failed_files.append((filename, str(exc)))
                    break

                delay = backoff_factors[attempt - 1]
                logging.warning(
                    "⚠ Connection or database error executing %s: %s. Retrying in %d seconds (attempt %d/%d)...",
                    filename, exc, delay, attempt, max_retries
                )
                time.sleep(delay)

    if failed_files:
        logging.error("\n" + "=" * 80)
        logging.error("✗ SQL EXECUTION SUMMARY: %d file(s) failed to execute", len(failed_files))
        for filename, err in failed_files:
            logging.error("  - %s: %s", filename, err)
        logging.error("=" * 80)
        raise RuntimeError(f"Failed to execute {len(failed_files)} SQL files: {[f[0] for f in failed_files]}")

    logging.info("✓ All SQL files executed successfully (%d files)", len(sql_files))


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def validate_all_tables(engine, schema: str, tables: dict) -> None:
    """
    Validate that every CSV-derived DataFrame schema matches the existing DB
    schema.  Raises ``RuntimeError`` with a detailed diff if any mismatch is
    found, so the pipeline aborts *before* touching the database.
    """
    errors = []

    for table_name, df in tables.items():
        db_struct = get_db_structure(engine, schema, table_name)

        if db_struct is None:
            logging.info("✓ Table %s.%s does not exist → will be created", schema, table_name)
            continue

        # row_hash is a loading-only column; exclude from schema comparison
        db_struct = [item for item in db_struct if item[0] != 'row_hash']

        # Migrate missing created_at column if needed
        db_cols = {col for col, _ in db_struct}
        if 'created_at' in df.columns and 'created_at' not in db_cols:
            logging.info("  → Adding missing 'created_at' column to %s.%s", schema, table_name)
            with engine.begin() as conn:
                conn.execute(text(
                    f'ALTER TABLE "{schema}"."{table_name}" '
                    f'ADD COLUMN created_at TIMESTAMP WITHOUT TIME ZONE'
                ))
            # Refresh cache so subsequent calls see the new column
            with _db_structure_cache_lock:
                _db_structure_cache.pop((schema, table_name), None)
            db_struct = get_db_structure(engine, schema, table_name)
            db_struct = [item for item in db_struct if item[0] != 'row_hash']

        # Auto-reorder CSV columns to match the DB column order when the sets
        # are identical — avoids false positives from insertion order differences
        db_cols_list = [col for col, _ in db_struct]
        if set(db_cols_list) == set(df.columns):
            df = df[db_cols_list]
            tables[table_name] = df

        df_struct = infer_df_structure(df)

        def is_column_empty(series: pd.Series) -> bool:
            return series.replace("", None).isna().all()

        schemas_match = True
        if len(db_struct) != len(df_struct):
            schemas_match = False
        else:
            for (db_col, db_type), (csv_col, csv_type) in zip(db_struct, df_struct):
                if db_col != csv_col:
                    schemas_match = False
                    break
                if db_type != csv_type and not is_column_empty(df[db_col]):
                    schemas_match = False
                    break

        if schemas_match:
            continue

        # Build a human-readable diff for the error message
        error_parts = [f"Schema mismatch for {schema}.{table_name}"]

        if len(db_struct) != len(df_struct):
            error_parts.append(
                f"  Column count: DB has {len(db_struct)}, CSV has {len(df_struct)}"
            )

        db_col_set = {col for col, _ in db_struct}
        csv_col_set = {col for col, _ in df_struct}
        missing_in_csv = db_col_set - csv_col_set
        extra_in_csv = csv_col_set - db_col_set

        if missing_in_csv:
            error_parts.append(f"  Columns in DB but missing in CSV: {sorted(missing_in_csv)}")
        if extra_in_csv:
            error_parts.append(f"  Columns in CSV but not in DB: {sorted(extra_in_csv)}")

        type_mismatches = []
        for i, ((db_col, db_type), (csv_col, csv_type)) in enumerate(zip(db_struct, df_struct)):
            if db_col == csv_col and db_type != csv_type:
                if not is_column_empty(df[db_col]):
                    type_mismatches.append(f"    Column '{db_col}': DB={db_type}, CSV={csv_type}")
            elif db_col != csv_col:
                type_mismatches.append(
                    f"    Position {i}: DB has '{db_col}' ({db_type}), CSV has '{csv_col}' ({csv_type})"
                )

        if type_mismatches:
            error_parts.append("  Type/Order mismatches:")
            error_parts.extend(type_mismatches)

        error_parts.append(f"\n  Full DB structure:\n    {db_struct}")
        error_parts.append(f"  Full CSV structure:\n    {df_struct}")
        errors.append("\n".join(error_parts))

    if errors:
        raise RuntimeError(
            "\n\n" + "=" * 80 + "\n"
            "SCHEMA VALIDATION FAILED\n"
            + "=" * 80 + "\n\n"
            + "\n\n".join(errors)
            + "\n\n" + "=" * 80 + "\n"
        )

    logging.info("✓ All table schemas validated successfully")


# ---------------------------------------------------------------------------
# Bulk load helpers
# ---------------------------------------------------------------------------

def truncate_table(engine, schema: str, table_name: str) -> None:
    """Truncate ``table_name`` if it exists (no-op otherwise)."""
    if get_db_structure(engine, schema, table_name) is None:
        logging.info("⊘ Skipping TRUNCATE for %s.%s (does not exist)", schema, table_name)
        return
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table_name}" RESTART IDENTITY CASCADE'))
    logging.info("✓ Truncated %s.%s", schema, table_name)


def coerce_df_to_db_schema(df: pd.DataFrame, db_struct: list) -> pd.DataFrame:
    """
    Coerce each DataFrame column to the type declared in the DB schema.

    Columns that do not yet exist in the DB (new additions) fall back to
    dynamic type promotion so they are still given sensible types.
    """
    db_type_map = {col: dtype for col, dtype in db_struct}

    for col in df.columns:
        if col not in db_type_map:
            # New column — promote dynamically
            temp = pd.DataFrame({col: df[col]})
            temp = promote_numeric_columns(temp)
            temp = promote_date_columns(temp)
            df[col] = temp[col]
            continue

        db_type = db_type_map[col].lower()

        if 'text' in db_type or 'char' in db_type or 'varchar' in db_type:
            df[col] = (
                df[col].fillna("").astype(str).str.strip()
                .replace({"nan": "", "NaT": "", "<NA>": ""})
            )
        elif 'int' in db_type or 'serial' in db_type:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        elif 'double' in db_type or 'real' in db_type or 'numeric' in db_type or 'decimal' in db_type:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        elif db_type == 'date':
            cleaned = df[col].astype(str).str.strip().replace(
                {"": None, "nan": None, "<NA>": None, "NaT": None}
            )
            parsed = pd.to_datetime(cleaned, format="%m/%d/%Y", errors="coerce")
            if parsed.isna().all() and cleaned.notna().any():
                parsed = pd.to_datetime(cleaned, errors="coerce")
            df[col] = parsed.dt.date
        elif 'timestamp' in db_type:
            cleaned = df[col].astype(str).str.strip().replace(
                {"": None, "nan": None, "<NA>": None, "NaT": None}
            )
            df[col] = pd.to_datetime(cleaned, errors="coerce")

    return df


def compute_row_hash(df: pd.DataFrame, exclude_cols: Optional[set] = None) -> pd.Series:
    """
    Compute an MD5 hash for each row using all columns except those in
    ``exclude_cols`` (defaults to ``{'created_at', 'row_hash'}``).

    Returns a Series of hex-digest strings.
    """
    if exclude_cols is None:
        exclude_cols = {'created_at', 'row_hash'}
    cols = [c for c in df.columns if c not in exclude_cols]

    if not cols:
        empty_digest = hashlib.md5(b'').hexdigest()
        return pd.Series([empty_digest] * len(df), index=df.index)

    # Build the '|'-joined row string column-at-a-time rather than with a row-wise
    # ``apply(axis=1)``. ``Series.map(str)`` reproduces the exact per-element string
    # of the old code (so hashes stay compatible with rows already in the DB), while
    # ``str.cat`` concatenates in vectorized C — far faster and with a fraction of the
    # peak memory of materializing one Python Series per row.
    parts = [df[c].map(str) for c in cols]
    combined = parts[0] if len(parts) == 1 else parts[0].str.cat(parts[1:], sep='|')
    return combined.map(lambda v: hashlib.md5(v.encode('utf-8')).hexdigest())


def ensure_row_hash_column(engine, schema: str, table_name: str) -> None:
    """
    Ensure the target table has a ``row_hash`` column with an index.
    Adds the column and index if either is missing.
    """
    with engine.begin() as conn:
        exists = conn.execute(text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table AND column_name = 'row_hash'"
        ), {"schema": schema, "table": table_name}).fetchone()

        if not exists:
            logging.info("  → Adding 'row_hash' column to %s.%s", schema, table_name)
            conn.execute(text(
                f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN row_hash TEXT'
            ))
            with _db_structure_cache_lock:
                _db_structure_cache.pop((schema, table_name), None)

        idx_name = f"idx_{table_name}_row_hash"
        idx_exists = conn.execute(text(
            "SELECT 1 FROM pg_indexes WHERE schemaname = :schema AND indexname = :idx"
        ), {"schema": schema, "idx": idx_name}).fetchone()

        if not idx_exists:
            logging.info("  → Creating index %s on %s.%s", idx_name, schema, table_name)
            conn.execute(text(
                f'CREATE INDEX "{idx_name}" ON "{schema}"."{table_name}" (row_hash)'
            ))


def copy_df_to_table(engine, schema: str, table_name: str, df: pd.DataFrame) -> None:
    """
    Bulk-load a DataFrame into PostgreSQL using ``COPY … FROM STDIN``.

    Uses tab as the delimiter to avoid quoting issues in text columns.
    This is significantly faster than ``to_sql()`` with ``method='multi'``.
    """
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, sep='\t', na_rep='\\N')
    buf.seek(0)

    cols = ', '.join(f'"{c}"' for c in df.columns)
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.copy_expert(
                f'COPY "{schema}"."{table_name}" ({cols}) '
                f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')",
                buf,
            )
        raw_conn.commit()
    finally:
        raw_conn.close()


# ---------------------------------------------------------------------------
# Results index (O(1) lookup for update_results_list)
# ---------------------------------------------------------------------------

def build_results_index(results_list: list) -> dict:
    """
    Build a dict keyed by ``(instance_key, report_name_upper, customer_account)``
    for O(1) lookup during the load phase.  The values are references to the
    original dicts in ``results_list``, so mutations are reflected directly.
    """
    return {
        (r['instance_key'], r['report_name'].upper(), str(r['customer_account'])): r
        for r in results_list
    }


def update_results_list(
    results_index: dict,
    instance_key: str,
    report_name: str,
    customer_account: str,
    rows_inserted: int = 0,
    rows_duplicate: int = 0,
    load_status: str = 'SUCCESS',
    load_error: Optional[str] = None,
) -> None:
    """
    Update load-phase statistics for a specific ``(instance, report, account)``
    combination using an O(1) index lookup.
    """
    key = (instance_key, report_name.upper(), str(customer_account))
    entry = results_index.get(key)
    if entry is not None:
        entry['rows_inserted'] = rows_inserted
        entry['rows_duplicate'] = rows_duplicate
        entry['load_status'] = load_status
        entry['load_error'] = load_error or ""


# ---------------------------------------------------------------------------
# Extract & Transform
# ---------------------------------------------------------------------------

def extract_and_transform_csvs(engine, schema: str, csv_files: list) -> dict:
    """
    Read CSV files, clean column names, coerce types, and merge DataFrames
    that map to the same table name.

    Returns ``{table_name: DataFrame}``.
    """
    tables: dict = {}

    for csv_file in csv_files:
        table_name = to_snake_case(os.path.splitext(os.path.basename(csv_file))[0])
        df = pd.read_csv(csv_file, low_memory=False)

        # Preserve the instance_key column before renaming (it uses a reserved name
        # but is a legitimate source column added by the fetch phase)
        instance_key_col = None
        if 'instance_key' in df.columns:
            instance_key_col = df['instance_key'].copy()
            df = df.drop(columns=['instance_key'])

        # Rename columns to snake_case; guard against reserved name collisions
        df.columns = [safe_column_name(c) for c in df.columns]

        # Coerce column types to the target DB schema when the table already exists
        db_struct = get_db_structure(engine, schema, table_name)
        if db_struct is not None:
            df = coerce_df_to_db_schema(df, db_struct)
        else:
            df = promote_numeric_columns(df)
            df = promote_date_columns(df)

        # Columns that are entirely null become empty strings (skip existing DB columns to preserve coerced null types)
        db_cols = {col for col, _ in db_struct} if db_struct is not None else set()
        for col in df.columns:
            if col not in db_cols and df[col].isna().all():
                df[col] = ""

        # Re-insert instance_key at position 1 (after customer_account)
        if instance_key_col is not None:
            df.insert(1, 'instance_key', instance_key_col)

        # Synthetic columns added by the ETL
        df['created_at'] = pd.Timestamp.now()

        # Merge with a previously loaded frame for the same table name
        if table_name in tables:
            existing_df = tables[table_name]
            existing_cols = set(existing_df.columns)
            new_cols = set(df.columns)

            # Strict structure validation: ensure identical sets of columns (ignoring order)
            if existing_cols != new_cols:
                missing_in_new = existing_cols - new_cols
                extra_in_new = new_cols - existing_cols
                error_parts = [
                    f"Structure mismatch for table '{table_name}' between merged files."
                ]
                if missing_in_new:
                    error_parts.append(f"  Missing columns in {csv_file}: {sorted(missing_in_new)}")
                if extra_in_new:
                    error_parts.append(f"  Extra columns in {csv_file}: {sorted(extra_in_new)}")
                raise ValueError("\n".join(error_parts))

            for col in existing_cols - new_cols:
                df[col] = pd.NA
                logging.warning("  ⚠ Added missing column '%s' to %s", col, csv_file)

            extra_cols = new_cols - existing_cols
            if extra_cols:
                logging.warning(
                    "  ⚠ Removing extra columns from %s: %s", csv_file, sorted(extra_cols)
                )
                df = df.drop(columns=extra_cols, errors='ignore')

            df = df[list(existing_df.columns)]
            df = pd.concat([existing_df, df], ignore_index=True)
            logging.info("✓ Merged CSV: %s (%d rows total in table '%s')", csv_file, len(df), table_name)
        else:
            logging.info("✓ Loaded CSV: %s (%d rows, %d columns)", csv_file, len(df), len(df.columns))

        tables[table_name] = df

    # Run a final coercion pass on the merged DataFrames to align types with target DB schema
    for t_name, t_df in tables.items():
        db_struct = get_db_structure(engine, schema, t_name)
        if db_struct is not None:
            tables[t_name] = coerce_df_to_db_schema(t_df, db_struct)

    return tables


# ---------------------------------------------------------------------------
# Load phase
# ---------------------------------------------------------------------------

def _transform_chunk(
    chunk: pd.DataFrame,
    engine,
    schema: str,
    table_name: str,
    db_struct: list,
    created_at: pd.Timestamp,
) -> pd.DataFrame:
    """
    Apply the same per-file transforms as :func:`extract_and_transform_csvs`
    (rename → coerce → synthetic columns) to a single CSV chunk.

    Only used by the streaming incremental loader, so ``db_struct`` is always a
    real (existing-table) schema and the null-column-to-empty-string fixup is a
    no-op — every source column already exists in the DB.
    """
    df = chunk

    # Preserve the instance_key column before renaming (reserved name, but a
    # legitimate source column added by the fetch phase).
    instance_key_col = None
    if 'instance_key' in df.columns:
        instance_key_col = df['instance_key'].copy()
        df = df.drop(columns=['instance_key'])

    df.columns = [safe_column_name(c) for c in df.columns]
    df = coerce_df_to_db_schema(df, db_struct)

    # Columns entirely null in this chunk but not part of the DB schema become
    # empty strings (mirrors the whole-file path; a no-op for existing tables).
    db_cols = {col for col, _ in db_struct}
    for col in df.columns:
        if col not in db_cols and df[col].isna().all():
            df[col] = ""

    if instance_key_col is not None:
        df.insert(1, 'instance_key', instance_key_col.values)

    df['created_at'] = created_at
    return df


def _copy_chunk(raw_conn, schema: str, table_name: str, df: pd.DataFrame) -> None:
    """COPY a single chunk into ``table_name`` on an already-open raw connection."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, sep='\t', na_rep='\\N')
    buf.seek(0)
    cols = ', '.join(f'"{c}"' for c in df.columns)
    with raw_conn.cursor() as cur:
        cur.copy_expert(
            f'COPY "{schema}"."{table_name}" ({cols}) '
            f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')",
            buf,
        )


def load_table_streaming(
    engine,
    schema: str,
    table_name: str,
    csv_files: list,
    db_struct: list,
    results_index: Optional[dict] = None,
) -> None:
    """
    Memory-bounded incremental load for a single existing table.

    Reads each CSV in ``CHUNK_SIZE`` chunks, streams them into an UNLOGGED staging
    table, then inserts only rows whose ``row_hash`` is new via a single anti-join.
    Peak memory is one chunk regardless of table size, so multi-million-row tables
    load without being OOM-killed. Semantically identical to the whole-file
    incremental path in :func:`load_tables_to_db`.
    """
    staging_table = f"{table_name}_staging"
    created_at = pd.Timestamp.now()

    max_retries = 3
    backoff_factors = [10, 30, 90]

    for attempt in range(1, max_retries + 1):
        try:
            ensure_row_hash_column(engine, schema, table_name)

            # UNLOGGED clone of the target — staging is transient, so skipping WAL makes the
            # bulk COPY (the dominant write) markedly faster. INCLUDING ALL copies the
            # row_hash index too, so the anti-join is indexed without an extra build step.
            with engine.begin() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging_table}"'))
                conn.execute(text(
                    f'CREATE UNLOGGED TABLE "{schema}"."{staging_table}" '
                    f'(LIKE "{schema}"."{table_name}" INCLUDING ALL)'
                ))

            total_counts: dict = {}   # (instance_key, customer_account) -> rows staged
            total_staged = 0
            insert_cols: Optional[list] = None
            has_account_cols = False

            raw_conn = engine.raw_connection()
            try:
                for csv_file in csv_files:
                    for chunk in pd.read_csv(csv_file, low_memory=False, chunksize=CHUNK_SIZE):
                        df = _transform_chunk(chunk, engine, schema, table_name, db_struct, created_at)
                        if df.empty:
                            continue

                        df['row_hash'] = compute_row_hash(df)

                        if insert_cols is None:
                            insert_cols = list(df.columns)
                            has_account_cols = (
                                'customer_account' in insert_cols and 'instance_key' in insert_cols
                            )

                        if has_account_cols:
                            grp = df.groupby(['instance_key', 'customer_account']).size()
                            for (inst, acc), n in grp.items():
                                key = (str(inst), str(acc))
                                total_counts[key] = total_counts.get(key, 0) + int(n)

                        total_staged += len(df)
                        _copy_chunk(raw_conn, schema, staging_table, df)
                        del df
                        gc.collect()
                raw_conn.commit()
            finally:
                try:
                    raw_conn.close()
                except Exception as close_err:
                    logging.debug("Error closing raw connection: %s", close_err)

            if total_staged == 0:
                logging.info("✓ Incremental load %s.%s: nothing to load (0 rows)", schema, table_name)
                with engine.begin() as conn:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging_table}"'))
                return

            cols_str = ", ".join(f'"{c}"' for c in insert_cols)
            select_str = ", ".join(f's."{c}"' for c in insert_cols)

            # Anti-join insert. The RETURNING rows are aggregated *inside Postgres* by the
            # wrapping CTE, so even a full first-time load never ships millions of rows
            # back to the client just to count them.
            if has_account_cols:
                insert_query = f"""
                    WITH ins AS (
                        INSERT INTO "{schema}"."{table_name}" ({cols_str})
                        SELECT {select_str}
                        FROM "{schema}"."{staging_table}" s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM "{schema}"."{table_name}" t
                            WHERE t.row_hash = s.row_hash
                        )
                        RETURNING instance_key, customer_account
                    )
                    SELECT instance_key, customer_account, count(*) AS n
                    FROM ins GROUP BY instance_key, customer_account;
                """
                with engine.begin() as conn:
                    returned = conn.execute(text(insert_query)).fetchall()
                new_counts = {(str(r[0]), str(r[1])): int(r[2]) for r in returned}
                inserted_rows = sum(new_counts.values())
            else:
                insert_query = f"""
                    WITH ins AS (
                        INSERT INTO "{schema}"."{table_name}" ({cols_str})
                        SELECT {select_str}
                        FROM "{schema}"."{staging_table}" s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM "{schema}"."{table_name}" t
                            WHERE t.row_hash = s.row_hash
                        )
                        RETURNING 1
                    )
                    SELECT count(*) FROM ins;
                """
                with engine.begin() as conn:
                    inserted_rows = int(conn.execute(text(insert_query)).scalar() or 0)
                new_counts = {}

            duplicate_rows = total_staged - inserted_rows
            logging.info(
                "✓ Incremental load %s.%s: %d inserted, %d duplicates skipped (%d total)",
                schema, table_name, inserted_rows, duplicate_rows, total_staged,
            )

            if results_index and has_account_cols:
                for (inst, acc), total in total_counts.items():
                    inserted = new_counts.get((inst, acc), 0)
                    update_results_list(
                        results_index, inst, table_name, acc,
                        rows_inserted=inserted,
                        rows_duplicate=max(0, total - inserted),
                        load_status='SUCCESS',
                    )

            # Successfully completed. Drop staging table and exit function.
            with engine.begin() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging_table}"'))
            return

        except Exception as exc:
            # Check if it's a transient db error
            is_trans = is_transient_db_error(exc)

            try:
                engine.dispose()
            except Exception as disp_err:
                logging.debug("Error disposing engine on retry: %s", disp_err)

            # Try to safely clean up the staging table, suppressing errors if the db is read-only
            try:
                with engine.begin() as conn:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging_table}"'))
            except Exception as drop_err:
                logging.debug("Staging table drop skipped or failed during retry block: %s", drop_err)

            if not is_trans or attempt == max_retries:
                if not is_trans:
                    logging.error("✗ Non-retryable error loading %s.%s: %s", schema, table_name, exc)
                else:
                    logging.error("✗ Failed to load %s.%s after %d attempts: %s", schema, table_name, max_retries, exc)
                raise

            delay = backoff_factors[attempt - 1]
            logging.warning(
                "⚠ Connection or database error loading %s.%s: %s. Retrying in %d seconds (attempt %d/%d)...",
                schema, table_name, exc, delay, attempt, max_retries
            )
            time.sleep(delay)


def load_tables_to_db(
    engine,
    schema: str,
    tables: dict,
    results_index: Optional[dict] = None,
    incremental: bool = True,
) -> None:
    """
    Load processed DataFrames into PostgreSQL.

    - ``incremental=True``: append only rows whose ``row_hash`` is new.
    - ``incremental=False``: truncate the table first, then bulk-COPY all rows.
    """
    for table_name, df in tables.items():
        max_retries = 3
        backoff_factors = [10, 30, 90]

        for attempt in range(1, max_retries + 1):
            try:
                # Invalidate cache so we see the freshest schema
                with _db_structure_cache_lock:
                    _db_structure_cache.pop((schema, table_name), None)
                db_struct = get_db_structure(engine, schema, table_name)

                if not incremental or db_struct is None:
                    # Full-refresh path (or brand-new table)
                    if db_struct is not None:
                        truncate_table(engine, schema, table_name)

                    df['row_hash'] = compute_row_hash(df)

                    if db_struct is None:
                        # Create the table shell from inferred dtypes
                        dtypes = get_sqlalchemy_dtypes(df)
                        df.head(0).to_sql(
                            table_name, engine, schema=schema,
                            if_exists="replace", index=False, dtype=dtypes,
                        )

                    copy_df_to_table(engine, schema, table_name, df)

                    with _db_structure_cache_lock:
                        _db_structure_cache.pop((schema, table_name), None)

                    logging.info("✓ Loaded %s.%s (%d rows)", schema, table_name, len(df))
                    ensure_row_hash_column(engine, schema, table_name)

                    # Update results summary
                    if results_index and 'customer_account' in df.columns and 'instance_key' in df.columns:
                        for (inst, acc), group in df.groupby(['instance_key', 'customer_account']):
                            update_results_list(
                                results_index, str(inst), table_name, str(acc),
                                rows_inserted=len(group), rows_duplicate=0, load_status='SUCCESS',
                            )

                else:
                    # Incremental path — use a staging table + hash-based anti-join
                    staging_table = f"{table_name}_staging"
                    try:
                        ensure_row_hash_column(engine, schema, table_name)
                        df['row_hash'] = compute_row_hash(df)

                        # Track total rows per account before the insert
                        total_counts: dict = {}
                        if 'customer_account' in df.columns and 'instance_key' in df.columns:
                            for (inst, acc), group in df.groupby(['instance_key', 'customer_account']):
                                total_counts[(str(inst), str(acc))] = len(group)

                        # Clone target table structure into a staging table
                        with engine.begin() as conn:
                            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging_table}"'))
                            conn.execute(text(
                                f'CREATE TABLE "{schema}"."{staging_table}" '
                                f'(LIKE "{schema}"."{table_name}" INCLUDING ALL)'
                            ))

                        copy_df_to_table(engine, schema, staging_table, df)

                        # Index staging table for the anti-join
                        with engine.begin() as conn:
                            conn.execute(text(
                                f'CREATE INDEX ON "{schema}"."{staging_table}" (row_hash)'
                            ))

                        insert_cols = [c for c in df.columns if c != 'row_hash']
                        all_cols_str = ", ".join(f'"{c}"' for c in insert_cols) + ', "row_hash"'
                        staging_cols_str = ", ".join(f's."{c}"' for c in insert_cols) + ', s."row_hash"'

                        has_account_cols = (
                            'customer_account' in df.columns and 'instance_key' in df.columns
                        )
                        returning_clause = (
                            ' RETURNING instance_key, customer_account' if has_account_cols else ''
                        )

                        insert_query = f"""
                            INSERT INTO "{schema}"."{table_name}" ({all_cols_str})
                            SELECT {staging_cols_str}
                            FROM "{schema}"."{staging_table}" s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM "{schema}"."{table_name}" t
                                WHERE t.row_hash = s.row_hash
                            ){returning_clause};
                        """

                        new_counts: dict = {}
                        with engine.begin() as conn:
                            result = conn.execute(text(insert_query))
                            if has_account_cols:
                                returned_rows = result.fetchall()
                                inserted_rows = len(returned_rows)
                                for r in returned_rows:
                                    key = (str(r[0]), str(r[1]))
                                    new_counts[key] = new_counts.get(key, 0) + 1
                            else:
                                inserted_rows = result.rowcount

                        duplicate_rows = len(df) - inserted_rows
                        logging.info(
                            "✓ Incremental load %s.%s: %d inserted, %d duplicates skipped (%d total)",
                            schema, table_name, inserted_rows, duplicate_rows, len(df),
                        )

                        if results_index and has_account_cols:
                            unique_groups = df[['instance_key', 'customer_account']].drop_duplicates()
                            for _, row in unique_groups.iterrows():
                                inst, acc = str(row['instance_key']), str(row['customer_account'])
                                total = total_counts.get((inst, acc), 0)
                                inserted = new_counts.get((inst, acc), 0)
                                update_results_list(
                                    results_index, inst, table_name, acc,
                                    rows_inserted=inserted,
                                    rows_duplicate=max(0, total - inserted),
                                    load_status='SUCCESS',
                                )

                    finally:
                        try:
                            with engine.begin() as conn:
                                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging_table}"'))
                        except Exception as drop_err:
                            logging.warning(
                                "⚠ Failed to drop staging table %s: %s", staging_table, drop_err
                            )
                # Successful load of this table, break retry loop to go to next table
                break

            except Exception as exc:
                is_trans = is_transient_db_error(exc)

                try:
                    engine.dispose()
                except Exception as disp_err:
                    logging.debug("Error disposing engine on retry: %s", disp_err)

                # Try to safely clean up the staging table, suppressing errors if the db is read-only
                staging_table = f"{table_name}_staging"
                try:
                    with engine.begin() as conn:
                        conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging_table}"'))
                except Exception as drop_err:
                    logging.debug("Staging table drop skipped or failed during retry block: %s", drop_err)

                if not is_trans or attempt == max_retries:
                    logging.error("✗ Failed to load %s.%s: %s", schema, table_name, exc)
                    if results_index and 'customer_account' in df.columns and 'instance_key' in df.columns:
                        unique_groups = df[['instance_key', 'customer_account']].drop_duplicates()
                        for _, row in unique_groups.iterrows():
                            update_results_list(
                                results_index, str(row['instance_key']), table_name,
                                str(row['customer_account']),
                                rows_inserted=0, rows_duplicate=0,
                                load_status='FAILED', load_error=str(exc),
                            )
                    raise

                delay = backoff_factors[attempt - 1]
                logging.warning(
                    "⚠ Connection or database error loading %s.%s: %s. Retrying in %d seconds (attempt %d/%d)...",
                    schema, table_name, exc, delay, attempt, max_retries
                )
                time.sleep(delay)


# ---------------------------------------------------------------------------
# Main ETL orchestration
# ---------------------------------------------------------------------------

def load_csvs_to_db(results_list: Optional[list] = None, incremental: bool = True) -> None:
    """
    Read all CSVs written by the fetch phase and load them into PostgreSQL.

    Two-pass approach (deliberate):
    - Pass 1 (Validation): transform every CSV and validate its schema against the
      DB.  This fails fast before any data is written.
    - Pass 2 (Loading):    re-transform each table's CSVs and bulk-COPY into the DB,
      freeing memory after each table.

    The double transform is intentional — it keeps peak memory proportional to
    the largest single table rather than the sum of all tables.
    """
    schema = postgres_config['schema']
    engine = create_engine(
        f"postgresql://{postgres_config['user']}:{postgres_config['password']}"
        f"@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}",
        pool_pre_ping=True,      # detect & replace dead connections on checkout
        pool_recycle=1800,       # recycle connections older than 30 min
        connect_args=DB_CONNECT_ARGS,
    )

    instance_list = config_loader.list_instances()
    if len(instance_list) > 1:
        csv_files = []
        for key in instance_list:
            csv_files.extend(glob.glob(f"csv_files/{key}/*.csv"))
    else:
        csv_files = glob.glob("csv_files/*.csv")

    logging.info("\n" + "=" * 80)
    logging.info("ETL PROCESSING PHASE (TABLE-BY-TABLE)")
    logging.info("=" * 80)
    logging.info("Found %d CSV file(s) to process.", len(csv_files))

    # Group files by target table name
    files_by_table: dict = {}
    for csv_file in csv_files:
        table_name = to_snake_case(os.path.splitext(os.path.basename(csv_file))[0])
        files_by_table.setdefault(table_name, []).append(csv_file)

    # ------------------------------------------------------------------
    # Phase 1: Validation — transform + schema-check each table, fail fast
    # ------------------------------------------------------------------
    logging.info("\n" + "=" * 80)
    logging.info("ETL VALIDATION PHASE (UPFRONT SCHEMA CHECKS)")
    logging.info("=" * 80)

    for table_name, table_csv_files in files_by_table.items():
        logging.info("Pre-validating schema for table: '%s'...", table_name)
        tables = extract_and_transform_csvs(engine, schema, table_csv_files)
        if tables and table_name in tables:
            validate_all_tables(engine, schema, tables)
        del tables
        gc.collect()

    logging.info("✓ All table schemas pre-validated. Starting database load...\n")

    # ------------------------------------------------------------------
    # Phase 2: Load — re-transform (to keep peak memory low) then bulk-COPY
    # ------------------------------------------------------------------
    logging.info("\n" + "=" * 80)
    logging.info("ETL LOADING PHASE (TABLE-BY-TABLE)")
    logging.info("=" * 80)

    # Build O(1) index once — used by load_tables_to_db to record stats
    results_index = build_results_index(results_list) if results_list else None

    for table_name, table_csv_files in files_by_table.items():
        logging.info("\nProcessing table: '%s' (%d file(s))", table_name, len(table_csv_files))
        logging.info("-" * 50)

        # Incremental loads of an existing table use the memory-bounded streaming
        # loader (chunked read → staging → anti-join). The whole-file path is kept
        # for full-refresh runs and brand-new tables, which need the complete frame
        # to infer/replace the schema; these are rare and typically small.
        db_struct = get_db_structure(engine, schema, table_name)
        if incremental and db_struct is not None:
            logging.info("Loading data for '%s'...", table_name)
            load_table_streaming(
                engine, schema, table_name, table_csv_files,
                db_struct, results_index=results_index,
            )
            gc.collect()
            continue

        tables = extract_and_transform_csvs(engine, schema, table_csv_files)
        if not tables or table_name not in tables:
            continue

        logging.info("Loading data for '%s'...", table_name)
        load_tables_to_db(engine, schema, tables, results_index=results_index, incremental=incremental)

        del tables
        gc.collect()

    run_sql_files(engine, schema)

    logging.info("\n" + "=" * 80)
    logging.info("ETL COMPLETE")
    logging.info("=" * 80)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    # Held for the whole run; released automatically when the process exits.
    _run_lock = acquire_run_lock()  # noqa: F841

    logging.info("\n" + "=" * 80)
    logging.info("STARTING ETL PIPELINE")
    logging.info("=" * 80)

    parser = argparse.ArgumentParser(description="Fetch and load reports ETL pipeline")
    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help="Perform a full refresh — truncate tables before loading",
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=24,
        help="Number of concurrent download threads (default: 24)",
    )
    args, _ = parser.parse_known_args()

    # Determine load strategy
    config_incremental = postgres_config.get('incremental', True)
    incremental = not args.full_refresh and config_incremental

    if args.full_refresh:
        logging.info("⚡ FORCING FULL REFRESH (truncating tables before loading)")
    elif not incremental:
        logging.info("⚡ Configured for FULL REFRESH (truncating tables before loading)")
    else:
        logging.info("Running INCREMENTAL LOAD (upserting unique rows only)")

    results_list = fetch_reports_to_csv(max_workers=args.workers)
    load_csvs_to_db(results_list=results_list, incremental=incremental)
    write_fetch_summary(results_list)


if __name__ == "__main__":
    _bootstrap()
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
        sys.exit(1)