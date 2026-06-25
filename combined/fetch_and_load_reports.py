import os
import hashlib
import io
import zipfile
import base64
from datetime import date, datetime
import time
import concurrent.futures
import threading
import sys
import argparse

import requests
import logging
import xml.etree.ElementTree as ET
import psycopg2
import csv
import pandas as pd
import re
from sqlalchemy import create_engine, text
import glob

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

setup_file_logging("fetch_and_load_reports.log")

from config_loader import ConfigLoader


# Load config using multi-instance aware loader
# Prefer the new Python config if present, otherwise fall back to the old INI
config_path = 'config/config.py'
config_loader = ConfigLoader(config_path)
postgres_config = config_loader.get_postgres_config()


def postgres_connection():
    return psycopg2.connect(
        host=postgres_config['host'],
        user=postgres_config['user'],
        password=postgres_config['password'],
        dbname=postgres_config['database'],
        port=postgres_config['port']
    )


def load_report_matrix(instance_key=None):
    """
    Load report matrix from database.
    If instance_key is provided, only load reports for that instance.
    Otherwise, load all reports.

    Returns: {
        'account_id': {
            'report_name': 'identifier'
        }
    }
    """
    conn = postgres_connection()
    cursor = conn.cursor()
    schema = postgres_config['schema']

    # Check if instance_key column exists
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = 'account_reports' AND column_name = 'instance_key'
    """, (schema,))

    has_instance_column = cursor.fetchone() is not None

    if has_instance_column and instance_key:
        cursor.execute(
            f"SELECT customer_account, report_name, identifier FROM {schema}.account_reports WHERE status = 1 AND instance_key = %s",
            (instance_key,)
        )
    else:
        cursor.execute(
            f"SELECT customer_account, report_name, identifier FROM {schema}.account_reports WHERE status = 1"
        )

    report_matrix = {}
    for customer_account, report_name, identifier in cursor.fetchall():
        report_matrix.setdefault(customer_account, {})[report_name] = identifier

    cursor.close()
    conn.close()
    return report_matrix


def find_element(root, local_name):
    """
    Finds the first child/descendant element matching the local name,
    ignoring namespaces.
    """
    for elem in root.iter():
        if elem.tag.split('}')[-1] == local_name:
            return elem
    return None


def has_instance_key_column(schema):
    """
    Checks if the account_reports table has an instance_key column.
    """
    conn = postgres_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = 'account_reports' AND column_name = 'instance_key'
    """, (schema,))
    has_column = cursor.fetchone() is not None
    cursor.close()
    conn.close()
    return has_column


def fetch_report_data_for_customer(base_url, username, password, customer_id, customer_name, report_id, report_name, instance_key, max_retries=100, retry_delay=60):
    """
    Fetches, decodes, and parses a single report result from CollaborateMD API.
    Returns (headers, list of rows, http_status, api_status, status_message, retries).
    """
    http_status = 0
    api_status = "UNKNOWN"
    status_message = "No response"
    retries = 0

    for attempt in range(max_retries):
        retries = attempt
        url = f"{base_url}/customer/{customer_id}/reports/results/{report_id}"
        try:
            response = requests.post(url, auth=(username, password))
            http_status = response.status_code
        except requests.exceptions.RequestException as e:
            api_status = "EXCEPTION"
            status_message = str(e)
            print(f"  → [ERROR] Request failed for {report_name} - account {customer_name} ({customer_id}): {e}")
            break

        if response.status_code != 200:
            api_status = f"HTTP_{response.status_code}"
            status_message = f"API call failed with status code {response.status_code}"
            print(f"  → [ERROR] Received status code {response.status_code} for {report_name} - account {customer_name} ({customer_id})")
            break

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as e:
            api_status = "XML_PARSE_ERROR"
            status_message = f"Failed to parse XML response: {e}"
            print(f"Error parsing XML response for {report_name} - account {customer_name} ({customer_id}): {e}")
            break

        # Find elements namespace-agnostically
        data_element = find_element(root, 'Data')
        status_element = find_element(root, 'Status')
        status_msg_element = find_element(root, 'StatusMessage')

        if status_msg_element is not None:
            status_message = status_msg_element.text or ""

        if status_element is not None:
            api_status = status_element.text or ""

        if status_element is not None and status_element.text == 'REPORT RUNNING':
            print(f"  → [RUNNING] {report_name} for account {customer_name} ({customer_id}) is still running. "
                  f"Waiting {retry_delay}s... (Attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            else:
                api_status = "TIMEOUT"
                print(f"  → [TIMEOUT] {report_name} for account {customer_name} ({customer_id}) did not complete in time.")
                break

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
                                return headers, rows, http_status, api_status, status_message, retries
            except Exception as e:
                api_status = "ZIP_CSV_ERROR"
                status_message = f"Failed to process zip/CSV: {e}"
                print(f"  → [ERROR] Failed to process zip/CSV for {report_name} - account {customer_name} ({customer_id}): {e}")
                break
            break
        else:
            xml_status = api_status
            api_status = "NO_DATA"
            status_info = f" (HTTP {http_status}, XML Status: {xml_status})"
            api_msg = f" - API Message: {status_message}" if status_message and status_message != "No response" else ""
            print(f"  → [WARNING] No data element found in response for {report_name} - account {customer_name} ({customer_id}).{status_info}{api_msg}")
            break

    return None, None, http_status, api_status, status_message, retries


csv_write_locks = {}
csv_write_locks_lock = threading.Lock()
_initialized_files = set()
_initialized_files_lock = threading.Lock()
_written_reports = set()
_written_reports_lock = threading.Lock()

def get_csv_write_lock(file_path):
    with csv_write_locks_lock:
        if file_path not in csv_write_locks:
            csv_write_locks[file_path] = threading.Lock()
        return csv_write_locks[file_path]

def write_report_to_csv(csv_dir, instance_key, report_name, headers, rows, instance_count):
    """
    Writes or appends report rows to a CSV file in a thread-safe manner.
    Creates subdirectories for isolation if multiple instances exist.
    """
    if instance_count > 1:
        instance_csv_dir = os.path.join(csv_dir, instance_key)
        os.makedirs(instance_csv_dir, exist_ok=True)
        file_path = os.path.join(instance_csv_dir, f"{report_name}.csv")
    else:
        file_path = os.path.join(csv_dir, f"{report_name}.csv")

    lock = get_csv_write_lock(file_path)
    with lock:
        with _initialized_files_lock:
            initialized = file_path in _initialized_files
            if not initialized:
                _initialized_files.add(file_path)

        mode = 'a' if initialized else 'w'
        with open(file_path, mode, newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not initialized:
                writer.writerow(headers)
            writer.writerows(rows)

    logging.info(f"CSV file {'appended' if initialized else 'written'}: {file_path}")
    print(f"✓ Fetched {report_name}: {len(rows)} rows from {len([r for r in rows if r[0]])} (appended for {instance_key})")


def collect_fetch_tasks(instance_list, instances, schema, has_instance_column):
    """
    Iterates over all instances and customer accounts to build a list of fetch tasks.
    """
    tasks = []
    for instance_key in instance_list:
        instance_config = instances[instance_key]
        base_url = instance_config['api_base_url']
        username = instance_config['username']
        password = instance_config['password']
        customers = instance_config['accounts']

        # Load reports for this instance
        report_matrix = load_report_matrix(instance_key if has_instance_column else None)
        account_names = instance_config.get('account_names', {})

        if not report_matrix:
            print(f"No reports found for instance {instance_key}")
            continue

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
                    'base_url': base_url,
                    'username': username,
                    'password': password
                })
    return tasks
def write_fetch_summary(results_list):
    """
    Writes the run metadata and results summary to a fetch summary CSV file.
    """
    if not results_list:
        return

    summary_dir = 'csv_files/fetch_summaries'
    os.makedirs(summary_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file = os.path.join(summary_dir, f"fetch_summary_{timestamp}.csv")
    latest_csv_file = os.path.join(summary_dir, "latest_fetch_summary.csv")

    fields = [
        'instance_key', 'customer_account', 'customer_name', 'report_name', 'report_id',
        'http_status', 'api_status', 'status_message', 'retries', 'rows_fetched', 'file_written',
        'load_status', 'rows_inserted', 'rows_duplicate', 'load_error'
    ]

    # Pre-fill defaults for items that didn't go through load phase
    for r in results_list:
        r.setdefault('load_status', 'SKIPPED' if r.get('file_written') == 'FALSE' else 'PENDING')
        r.setdefault('rows_inserted', 0)
        r.setdefault('rows_duplicate', 0)
        r.setdefault('load_error', '')

    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(results_list)
        print(f"\n✓ Generated fetch summary written to: {csv_file}")
    except Exception as e:
        print(f"\n✗ Failed to write fetch summary CSV: {e}")

    try:
        with open(latest_csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(results_list)
        print(f"✓ Copied latest fetch summary to: {latest_csv_file}")
    except Exception as e:
        print(f"✗ Failed to write latest summary CSV: {e}")

def fetch_reports_to_csv(max_workers=8):
    """
    Fetch reports for all instances and write to CSV files in parallel.
    Creates separate CSV files per instance if multiple instances exist.
    """
    instances = config_loader.get_instances()
    instance_list = config_loader.list_instances()
    schema = postgres_config['schema']

    print(f"\n{'=' * 80}")
    print(f"FETCH REPORTS TO CSV - MULTI-INSTANCE MODE")
    print(f"{'=' * 80}")
    print(f"Processing {len(instance_list)} instance(s): {', '.join(instance_list)}\n")

    has_instance_column = has_instance_key_column(schema)

    csv_dir = 'csv_files'
    os.makedirs(csv_dir, exist_ok=True)

    tasks = collect_fetch_tasks(instance_list, instances, schema, has_instance_column)

    if not tasks:
        print("No tasks to execute.")
        return []

    results_list = []
    results_lock = threading.Lock()

    # Clear tracking structures for this fetch run
    with _initialized_files_lock:
        _initialized_files.clear()
    with _written_reports_lock:
        _written_reports.clear()

    def worker(task):
        instance_key = task['instance_key']
        customer_id = task['customer_id']
        customer_name = task['customer_name']
        report_name = task['report_name']
        report_id = task['report_id']
        base_url = task['base_url']
        username = task['username']
        password = task['password']

        headers, rows, http_status, api_status, status_message, retries = fetch_report_data_for_customer(
            base_url, username, password, customer_id, customer_name, report_id, report_name, instance_key
        )

        rows_fetched = len(rows) if rows else 0

        # Add to results_list for fetch summary report
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
                'rows_fetched': rows_fetched
            })

        if rows:
            full_headers = ['customer_account', 'instance_key'] + headers if headers else []
            write_report_to_csv(csv_dir, instance_key, report_name, full_headers, rows, len(instance_list))
            with _written_reports_lock:
                _written_reports.add((instance_key, report_name.upper()))

    # Run tasks concurrently
    max_workers = min(max_workers, max(1, len(tasks)))
    print(f"Fetching {len(tasks)} reports in parallel using {max_workers} threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(worker, tasks)

    # Update file_written status in results_list
    with _written_reports_lock:
        for result in results_list:
            key = (result['instance_key'], result['report_name'])
            result['file_written'] = 'TRUE' if key in _written_reports else 'FALSE'

    return results_list


def to_snake_case(name):
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.lower().replace(' ', '_')
    name = re.sub(r'_+', '_', name)
    return name.strip('_') if name.strip('_') else "unnamed_column"


# ---------- Helpers ----------

_db_structure_cache = {}
_db_structure_cache_lock = threading.Lock()

def get_db_structure(engine, schema, table_name):
    """
    Returns a list of (column_name, data_type) for a table.
    Returns None if the table does not exist.
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
            ORDER BY ordinal_position \
            """
    with engine.connect() as conn:
        rows = conn.execute(
            text(query),
            {"schema": schema, "table": table_name}
        ).fetchall()

    result = None
    if rows:
        result = [(r.column_name, r.data_type) for r in rows]

    with _db_structure_cache_lock:
        _db_structure_cache[cache_key] = result

    return result


def promote_numeric_columns(df):
    """
    Attempt to promote text columns to numeric types.
    This ensures consistent type inference across runs.

    A column is NOT promoted if any non-empty value has a leading zero followed by
    another digit (e.g. "0301", "0450"). Real measurements never have leading zeros;
    such values are identifiers/codes and must stay as text to avoid silent data loss.
    """
    leading_zero_re = re.compile(r'^0\d')

    for col in df.columns:
        # Skip if already numeric or date
        if df[col].dtype != 'object':
            continue

        # Skip if it's a date column (will be handled by promote_date_columns)
        if 'date' in col.lower():
            continue

        # Try converting to numeric
        numeric_series = pd.to_numeric(df[col], errors='coerce')

        # Count how many values successfully converted
        non_null_original = df[col].notna() & (df[col] != '')
        non_null_converted = numeric_series.notna()

        # If all non-empty values successfully converted to numeric, use it
        # Use numpy boolean arrays to avoid static-analysis confusion about Series/bool
        mask = non_null_original
        mask_arr = mask.to_numpy(dtype=bool)
        conv_arr = non_null_converted.to_numpy(dtype=bool)
        num_mask = int(mask_arr.sum())
        if num_mask > 0:
            num_converted = int((conv_arr & mask_arr).sum())
            if num_converted == num_mask:
                # Guard: if any value has a leading zero before another digit (e.g. "0301"),
                # it's an identifier — promoting to float would silently destroy that zero.
                has_leading_zero = (
                    df[col][non_null_original]
                    .astype(str)
                    .str.strip()
                    .str.match(r'^0\d')
                    .any()
                )
                if not has_leading_zero:
                    # If the converted series is float64 but only contains integer values,
                    # cast it to Pandas modern nullable integer 'Int64' to prevent float64/double precision promotion.
                    if numeric_series.dtype == 'float64':
                        non_null_vals = numeric_series.dropna()
                        if len(non_null_vals) > 0 and (non_null_vals % 1 == 0).all():
                            numeric_series = numeric_series.astype('Int64')
                    df[col] = numeric_series


    return df



def promote_date_columns(df):
    """
    Promote column to date if:
    1. Column name contains 'date', OR
    2. All non-null/non-empty values match mm/dd/yyyy format
    """
    for col in df.columns:
        # Treat empty strings as NaN
        cleaned = df[col].replace("", pd.NA)

        # Skip if all values are null/empty
        if cleaned.isna().all():
            continue

        # Try to parse as date
        parsed = pd.to_datetime(cleaned, format="%m/%d/%Y", errors="coerce")

        # Check if all non-null values successfully parsed
        non_null_mask = cleaned.notna()
        invalid = non_null_mask & parsed.isna()

        # If all non-null values parsed successfully, it's a date column
        if not invalid.any():
            # Check if column name contains 'date' OR if data looks like dates
            has_date_in_name = "date" in col.lower()

            # Additional check: do the values actually look like dates?
            # Sample a few non-null values to verify they match mm/dd/yyyy pattern
            sample_values = cleaned.dropna().head(10)
            looks_like_date = False

            if len(sample_values) > 0:
                # Check if values match mm/dd/yyyy pattern (rough check)
                date_pattern = re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$')
                looks_like_date = sample_values.astype(str).apply(
                    lambda x: bool(date_pattern.match(x))
                ).any()

            # Convert to date if column name suggests it OR data looks like dates
            if has_date_in_name or looks_like_date:
                df[col] = parsed.dt.date
                print(f"  → Converted '{col}' to date type")

    return df


def infer_df_structure(df):
    """
    Infer expected DB structure from DataFrame based on actual pandas dtypes.
    This matches what pandas.to_sql() will create.

    Robust rules (to avoid misclassifying pandas Timestamp as date):
    1. Special-case 'created_at' -> timestamp without time zone
    2. If pandas dtype is datetime64 -> timestamp without time zone
    3. If dtype is integer/nullable-int -> bigint
    4. If dtype is float64 -> double precision
    5. If values are python date objects (but NOT datetime) -> date
    6. Otherwise -> text
    """
    structure = []
    for col in df.columns:
        dtype = df[col].dtype

        # Special-case created_at: always map to timestamp
        if col.lower() == 'created_at':
            structure.append((col, "timestamp without time zone"))
            continue

        # If pandas has datetime64 dtype, map to timestamp
        if pd.api.types.is_datetime64_any_dtype(dtype):
            structure.append((col, "timestamp without time zone"))
            continue

        # Integer types
        if dtype == 'int64' or dtype == 'Int64':
            structure.append((col, "bigint"))
            continue

        # Floats
        if dtype == 'float64':
            structure.append((col, "double precision"))
            continue

        # Check for python.date-only values (exclude datetime)
        is_date_only = False
        non_null = df[col].dropna()
        if len(non_null) > 0:
            # Check a small sample first (fast path)
            sample = non_null.head(100)
            try:
                sample_ok = all(isinstance(v, date) and not isinstance(v, datetime) for v in sample)
                if sample_ok:
                    if len(non_null) <= 100:
                        is_date_only = True
                    else:
                        is_date_only = non_null.apply(lambda v: isinstance(v, date) and not isinstance(v, datetime)).all()
            except Exception:
                is_date_only = False

        if is_date_only:
            structure.append((col, "date"))
            continue

        # Fallback to text
        structure.append((col, "text"))

    return structure


def run_sql_files(engine, schema, sql_folder='sql'):
    """
    Execute all SQL files in the specified folder.
    """
    print("\n" + "=" * 80)
    print("RUNNING SQL FILES")
    print("=" * 80 + "\n")

    # Get all .sql files in the folder
    sql_files = glob.glob(os.path.join(sql_folder, '*.sql'))

    if not sql_files:
        print(f"⚠ No SQL files found in '{sql_folder}' folder")
        return

    # Sort files for consistent execution order
    sql_files.sort()

    for sql_file in sql_files:
        print(f"Executing: {os.path.basename(sql_file)}")

        try:
            with open(sql_file, 'r') as f:
                sql = f.read()

            # Prepend search_path setting
            sql = f"SET search_path TO {schema};\n" + sql

            with engine.begin() as conn:
                conn.execute(text(sql))

            print(f"✓ Successfully executed: {os.path.basename(sql_file)}")

        except Exception as e:
            print(f"✗ Failed to execute {os.path.basename(sql_file)}: {str(e)}")
            raise

    print(f"\n✓ All SQL files executed successfully ({len(sql_files)} files)")


def validate_all_tables(engine, schema, tables):
    """
    Validates that CSV schemas exactly match DB schemas.
    Fails if:
    - Column names differ
    - Column order differs
    - Data types differ
    """
    errors = []

    for table_name, df in tables.items():
        db_struct = get_db_structure(engine, schema, table_name)

        if db_struct is None:
            print(f"✓ Table {schema}.{table_name} does not exist → will be created")
            continue2

        # Check for created_at mismatch specifically
        db_cols = {col for col, _ in db_struct}
        if 'created_at' in df.columns and 'created_at' not in db_cols:
            print(f"  → Adding missing 'created_at' column to {schema}.{table_name}")
            with engine.begin() as conn:
                conn.execute(text(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN created_at TIMESTAMP WITHOUT TIME ZONE'))
            # Refresh db_struct
            db_struct = get_db_structure(engine, schema, table_name)

        # Auto-reorder CSV columns to match DB column order if they contain the exact same set
        db_cols_list = [col for col, _ in db_struct]
        db_cols_set = set(db_cols_list)
        csv_cols_set = set(df.columns)
        if db_cols_set == csv_cols_set:
            df = df[db_cols_list]
            tables[table_name] = df

        df_struct = infer_df_structure(df)


        if db_struct != df_struct:
            # Build detailed error message
            error_parts = [f"Schema mismatch for {schema}.{table_name}"]

            # Check column count
            if len(db_struct) != len(df_struct):
                error_parts.append(
                    f"  Column count mismatch: DB has {len(db_struct)} columns, CSV has {len(df_struct)} columns"
                )

            # Check for missing/extra columns
            db_cols = {col for col, _ in db_struct}
            csv_cols = {col for col, _ in df_struct}

            missing_in_csv = db_cols - csv_cols
            extra_in_csv = csv_cols - db_cols

            if missing_in_csv:
                error_parts.append(f"  Columns in DB but missing in CSV: {sorted(missing_in_csv)}")
            if extra_in_csv:
                error_parts.append(f"  Columns in CSV but not in DB: {sorted(extra_in_csv)}")

            # Check for type mismatches in common columns
            type_mismatches = []
            for i, ((db_col, db_type), (csv_col, csv_type)) in enumerate(zip(db_struct, df_struct)):
                if db_col == csv_col and db_type != csv_type:
                    type_mismatches.append(
                        f"    Column '{db_col}': DB={db_type}, CSV={csv_type}"
                    )
                elif db_col != csv_col:
                    # Position mismatch
                    type_mismatches.append(
                        f"    Position {i}: DB has '{db_col}' ({db_type}), CSV has '{csv_col}' ({csv_type})"
                    )

            if type_mismatches:
                error_parts.append("  Type/Order mismatches:")
                error_parts.extend(type_mismatches)

            # Show full structures for comparison
            error_parts.append(f"\n  Full DB structure:\n    {db_struct}")
            error_parts.append(f"  Full CSV structure:\n    {df_struct}")

            errors.append("\n".join(error_parts))

    if errors:
        raise RuntimeError(
            "\n\n" + "=" * 80 + "\n" +
            "SCHEMA VALIDATION FAILED\n" +
            "=" * 80 + "\n\n" +
            "\n\n".join(errors) +
            "\n\n" + "=" * 80 + "\n"
        )

    print("✓ All table schemas validated successfully")


def truncate_table(engine, schema, table_name):
    """Truncate table if it exists"""
    db_struct = get_db_structure(engine, schema, table_name)
    if db_struct is None:
        print(f"⊘ Skipping TRUNCATE for {schema}.{table_name} (does not exist)")
        return

    with engine.begin() as conn:
        conn.execute(
            text(f'TRUNCATE TABLE "{schema}"."{table_name}" RESTART IDENTITY CASCADE')
        )
    print(f"✓ Truncated {schema}.{table_name}")


def coerce_df_to_db_schema(df, db_struct):
    """
    Coerces DataFrame column types to match the expected DB schema types.
    This avoids hardcoding column exclusions and prevents validation failures.
    """
    db_type_map = {col: dtype for col, dtype in db_struct}
    
    for col in df.columns:
        if col in db_type_map:
            db_type = db_type_map[col].lower()
            
            # 1. Text / Char types: Keep as string, just fillna/replace to avoid 'nan'
            if 'text' in db_type or 'char' in db_type or 'varchar' in db_type:
                df[col] = df[col].fillna("").astype(str).str.strip().replace({"nan": "", "NaT": "", "<NA>": ""})
                
            # 2. Integer types: Coerce to nullable Int64
            elif 'int' in db_type or 'serial' in db_type:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                
            # 3. Float / Numeric types: Coerce to float64
            elif 'double' in db_type or 'real' in db_type or 'numeric' in db_type or 'decimal' in db_type:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
                
            # 4. Date types: Parse as date
            elif 'date' == db_type:
                cleaned = df[col].astype(str).str.strip().replace({"": None, "nan": None, "<NA>": None, "NaT": None})
                parsed = pd.to_datetime(cleaned, format="%m/%d/%Y", errors="coerce")
                if parsed.isna().all() and cleaned.notna().any():
                    parsed = pd.to_datetime(cleaned, errors="coerce")
                df[col] = parsed.dt.date
                
            # 5. Timestamp types: Parse as datetime
            elif 'timestamp' in db_type:
                cleaned = df[col].astype(str).str.strip().replace({"": None, "nan": None, "<NA>": None, "NaT": None})
                df[col] = pd.to_datetime(cleaned, errors="coerce")
        else:
            # Column is new and not in the DB yet: run dynamic promotion on it
            temp_df = pd.DataFrame({col: df[col]})
            temp_df = promote_numeric_columns(temp_df)
            temp_df = promote_date_columns(temp_df)
            df[col] = temp_df[col]
            
    return df


# ---------- Main ETL ----------

def extract_and_transform_csvs(engine, schema, csv_files):
    """
    Extracts data from the CSV files, cleans columns, coerces types,
    and merges dataframes for identical tables.
    """
    tables = {}
    for csv_file in csv_files:
        table_name = to_snake_case(os.path.splitext(os.path.basename(csv_file))[0])
        df = pd.read_csv(csv_file, low_memory=False)

        # Preserve instance_key column if it exists
        instance_key_col = None
        if 'instance_key' in df.columns:
            instance_key_col = df['instance_key'].copy()
            df = df.drop(columns=['instance_key'])

        df.columns = [to_snake_case(c) for c in df.columns]

        # Coerce types to match target DB schema if table exists, else fallback to dynamic promotion
        db_struct = get_db_structure(engine, schema, table_name)
        if db_struct is not None:
            df = coerce_df_to_db_schema(df, db_struct)
        else:
            df = promote_numeric_columns(df)
            df = promote_date_columns(df)

        # Replace full-null columns
        for col in df.columns:
            if df[col].isna().all():
                df[col] = ""

        # Add instance_key column back if it existed
        if instance_key_col is not None:
            df.insert(1, 'instance_key', instance_key_col)

        # Add created_at column
        df['created_at'] = pd.Timestamp.now()

        # Merge with existing dataframe if table name already exists
        if table_name in tables:
            existing_df = tables[table_name]
            existing_cols = set(existing_df.columns)
            new_cols = set(df.columns)

            # Check for column mismatches
            missing_cols = existing_cols - new_cols
            extra_cols = new_cols - existing_cols

            if missing_cols:
                for col in missing_cols:
                    df[col] = ""
                print(f"  ⚠ Added missing columns to {csv_file}: {', '.join(sorted(missing_cols))}")

            if extra_cols:
                print(f"  ⚠ Removing extra columns from {csv_file}: {', '.join(sorted(extra_cols))}")
                df = df.drop(columns=extra_cols, errors='ignore')

            # Ensure column order matches existing dataframe
            df = df[list(existing_df.columns)]

            # Safe to merge
            df = pd.concat([existing_df, df], ignore_index=True)
            print(f"✓ Merged CSV: {csv_file} ({len(df)} rows total in table '{table_name}')")
        else:
            print(f"✓ Loaded CSV: {csv_file} ({len(df)} rows, {len(df.columns)} columns)")

        tables[table_name] = df
    return tables


def update_results_list(results_list, instance_key, report_name, customer_account, rows_inserted=0, rows_duplicate=0, load_status='SUCCESS', load_error=None):
    if not results_list:
        return
    for r in results_list:
        if (r['instance_key'] == instance_key and 
            r['report_name'] == report_name.upper() and 
            str(r['customer_account']) == str(customer_account)):
            r['rows_inserted'] = rows_inserted
            r['rows_duplicate'] = rows_duplicate
            r['load_status'] = load_status
            r['load_error'] = load_error or ""
            return
def compute_row_hash(df, exclude_cols=None):
    """
    Compute an MD5 hash for each row based on all columns except those in exclude_cols.
    Returns a Series of hex digest strings.
    """
    if exclude_cols is None:
        exclude_cols = {'created_at', 'row_hash'}
    cols = [c for c in df.columns if c not in exclude_cols]
    # Convert each row to a pipe-delimited string, then hash
    str_df = df[cols].astype(str)
    combined = str_df.apply(lambda row: '|'.join(row), axis=1)
    return combined.apply(lambda v: hashlib.md5(v.encode('utf-8')).hexdigest())


def ensure_row_hash_column(engine, schema, table_name):
    """
    Ensure the target table has a row_hash column with an index.
    Adds the column and index if they don't exist.
    """
    with engine.begin() as conn:
        # Check if column exists
        result = conn.execute(text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table AND column_name = 'row_hash'"
        ), {"schema": schema, "table": table_name}).fetchone()

        if not result:
            print(f"  → Adding 'row_hash' column to {schema}.{table_name}")
            conn.execute(text(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN row_hash TEXT'))
            with _db_structure_cache_lock:
                _db_structure_cache.pop((schema, table_name), None)

        # Check if index exists
        idx_name = f"idx_{table_name}_row_hash"
        idx_result = conn.execute(text(
            "SELECT 1 FROM pg_indexes WHERE schemaname = :schema AND indexname = :idx"
        ), {"schema": schema, "idx": idx_name}).fetchone()

        if not idx_result:
            print(f"  → Creating index {idx_name} on {schema}.{table_name}")
            conn.execute(text(f'CREATE INDEX "{idx_name}" ON "{schema}"."{table_name}" (row_hash)'))


def load_tables_to_db(engine, schema, tables, results_list=None, incremental=True):
    """
    Loads the processed DataFrames into PostgreSQL.
    If incremental is True, appends only new rows that don't already exist.
    If incremental is False, truncates and loads.
    """
    for table_name, df in tables.items():
        try:
            with _db_structure_cache_lock:
                _db_structure_cache.pop((schema, table_name), None)
            db_struct = get_db_structure(engine, schema, table_name)
            
            if not incremental or db_struct is None:
                # Full refresh or table doesn't exist yet
                if db_struct is not None:
                    truncate_table(engine, schema, table_name)
                # Compute row_hash before inserting
                df['row_hash'] = compute_row_hash(df)
                df.to_sql(
                    table_name,
                    engine,
                    schema=schema,
                    if_exists="append",
                    index=False,
                    method='multi',
                    chunksize=5000
                )
                with _db_structure_cache_lock:
                    _db_structure_cache.pop((schema, table_name), None)
                print(f"✓ Loaded {schema}.{table_name} ({len(df)} rows)")
                
                # Ensure index exists on row_hash for future incremental loads
                if db_struct is not None:
                    ensure_row_hash_column(engine, schema, table_name)
                
                # Update stats in results_list
                if 'customer_account' in df.columns and 'instance_key' in df.columns:
                    unique_groups = df.groupby(['instance_key', 'customer_account'])
                    for (inst, acc), group in unique_groups:
                        update_results_list(
                            results_list, str(inst), table_name, str(acc),
                            rows_inserted=len(group), rows_duplicate=0, load_status='SUCCESS'
                        )
            else:
                # Incremental mode using row_hash for fast comparison
                staging_table = f"{table_name}_staging"
                try:
                    # Ensure target table has row_hash column + index
                    ensure_row_hash_column(engine, schema, table_name)

                    # Compute row_hash for the staging data
                    df['row_hash'] = compute_row_hash(df)

                    # Count total staged rows per account (before insert)
                    total_counts = {}
                    if 'customer_account' in df.columns and 'instance_key' in df.columns:
                        for (inst, acc), group in df.groupby(['instance_key', 'customer_account']):
                            total_counts[(str(inst), str(acc))] = len(group)

                    # Write staging table
                    df.to_sql(
                        staging_table,
                        engine,
                        schema=schema,
                        if_exists="replace",
                        index=False,
                        method='multi',
                        chunksize=5000
                    )

                    # Create index on staging table row_hash for the anti-join
                    with engine.begin() as conn:
                        conn.execute(text(f'CREATE INDEX ON "{schema}"."{staging_table}" (row_hash)'))

                    # Build column list (excluding row_hash for the final insert into target)
                    insert_cols = [c for c in df.columns if c != 'row_hash']
                    all_cols_str = ", ".join([f'"{c}"' for c in insert_cols]) + ', "row_hash"'
                    staging_cols_str = ", ".join([f's."{c}"' for c in insert_cols]) + ', s."row_hash"'

                    # Single INSERT with hash-based NOT EXISTS and RETURNING clause for accurate stats
                    has_account_cols = 'customer_account' in df.columns and 'instance_key' in df.columns
                    returning_clause = ' RETURNING s.instance_key, s.customer_account' if has_account_cols else ''

                    insert_query = f"""
                        INSERT INTO "{schema}"."{table_name}" ({all_cols_str})
                        SELECT {staging_cols_str}
                        FROM "{schema}"."{staging_table}" s
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM "{schema}"."{table_name}" t
                            WHERE t.row_hash = s.row_hash
                        ){returning_clause};
                    """

                    new_counts = {}
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
                    print(f"✓ Incrementally loaded {schema}.{table_name}: "
                          f"inserted {inserted_rows} new rows, "
                          f"{duplicate_rows} duplicates skipped "
                          f"(out of {len(df)} total fetched)")

                    # Update results_list with accurate counts
                    if has_account_cols:
                        unique_groups = df[['instance_key', 'customer_account']].drop_duplicates()
                        for _, row in unique_groups.iterrows():
                            inst = str(row['instance_key'])
                            acc = str(row['customer_account'])
                            total = total_counts.get((inst, acc), 0)
                            inserted = new_counts.get((inst, acc), 0)
                            update_results_list(
                                results_list, inst, table_name, acc,
                                rows_inserted=inserted,
                                rows_duplicate=max(0, total - inserted),
                                load_status='SUCCESS'
                            )
                finally:
                    # Clean up staging table
                    try:
                        with engine.begin() as conn:
                            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging_table}"'))
                    except Exception as drop_err:
                        print(f"⚠ Warning: failed to drop staging table {staging_table}: {drop_err}")
        except Exception as e:
            print(f"✗ Failed to load {schema}.{table_name}: {str(e)}")
            # Mark all accounts/instances for this table as failed in results_list
            if results_list and 'customer_account' in df.columns and 'instance_key' in df.columns:
                unique_groups = df[['instance_key', 'customer_account']].drop_duplicates()
                for _, row in unique_groups.iterrows():
                    update_results_list(
                        results_list, str(row['instance_key']), table_name, str(row['customer_account']),
                        rows_inserted=0, rows_duplicate=0, load_status='FAILED', load_error=str(e)
                    )
            engine.dispose()  # Close all connections
            raise


def load_csvs_to_db(results_list=None, incremental=True):
    schema = postgres_config['schema']

    engine = create_engine(
        f"postgresql://{postgres_config['user']}:"
        f"{postgres_config['password']}@"
        f"{postgres_config['host']}:"
        f"{postgres_config['port']}/"
        f"{postgres_config['database']}"
    )

    # Get CSV files for active instances (or root csv_files if single instance), avoiding summaries
    instance_list = config_loader.list_instances()
    csv_files = []
    if len(instance_list) > 1:
        for instance_key in instance_list:
            csv_files.extend(glob.glob(f"csv_files/{instance_key}/*.csv"))
    else:
        csv_files = glob.glob("csv_files/*.csv")

    print("\n" + "=" * 80)
    print("ETL PROCESSING PHASE (TABLE-BY-TABLE)")
    print("=" * 80 + "\n")
    print(f"Found {len(csv_files)} CSV file(s) to process.")

    # Group CSV files by table name
    files_by_table = {}
    for csv_file in csv_files:
        table_name = to_snake_case(os.path.splitext(os.path.basename(csv_file))[0])
        files_by_table.setdefault(table_name, []).append(csv_file)

    for table_name, table_csv_files in files_by_table.items():
        print(f"\nProcessing table: '{table_name}' with {len(table_csv_files)} file(s)")
        print("-" * 50)
        
        # 1. Extract and transform CSVs for this table
        tables = extract_and_transform_csvs(engine, schema, table_csv_files)
        if not tables or table_name not in tables:
            continue
            
        # 2. Validate schema for this table
        print(f"Validating schema for '{table_name}'...")
        validate_all_tables(engine, schema, tables)
        
        # 3. Load data for this table
        print(f"Loading data for '{table_name}'...")
        load_tables_to_db(engine, schema, tables, results_list=results_list, incremental=incremental)
        
        # Free memory and trigger garbage collection
        del tables
        import gc
        gc.collect()

    run_sql_files(engine, schema)

    print("\n" + "=" * 80)
    print("ETL COMPLETE")
    print("=" * 80 + "\n")



def main():
    print("\n" + "=" * 80)
    print("STARTING ETL PIPELINE")
    print("=" * 80 + "\n")

    # Parse command line args
    parser = argparse.ArgumentParser(description="Fetch and load reports ETL pipeline")
    parser.add_argument('--full-refresh', action='store_true', help="Perform full refresh (truncate tables first)")
    parser.add_argument('--workers', type=int, default=8, help="Number of concurrent download threads (default: 8)")
    args, unknown = parser.parse_known_args()

    # Determine load strategy (default to config parameter or True if not specified)
    config_incremental = postgres_config.get('incremental', True)
    incremental = not args.full_refresh and config_incremental

    if args.full_refresh:
        print("⚡ FORCING FULL REFRESH (truncating tables before loading)")
    elif not incremental:
        print("⚡ Configured for FULL REFRESH (truncating tables before loading)")
    else:
        print("🔄 Running INCREMENTAL LOAD (upserting unique rows only)")

    results_list = fetch_reports_to_csv(max_workers=args.workers)

    load_csvs_to_db(results_list=results_list, incremental=incremental)

    write_fetch_summary(results_list)


if __name__ == "__main__":
    main()