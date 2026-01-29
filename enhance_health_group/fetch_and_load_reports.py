import os
import io
import zipfile
import base64
from datetime import date

import requests
import logging
import ast
import xml.etree.ElementTree as ET
import psycopg2
from configparser import RawConfigParser
import csv
import pandas as pd
import re
from sqlalchemy import create_engine, text
import glob

# Load config
config = RawConfigParser()
config.read('config/config.ini')


# Override with environment variables if available
def get_config(section, key, fallback=None):
    env_key = f"{section}_{key}".upper()
    return os.getenv(env_key, config.get(section, key, fallback=fallback))


def postgres_connection():
    return psycopg2.connect(
        host=get_config('POSTGRES', 'host'),
        user=get_config('POSTGRES', 'user'),
        password=get_config('POSTGRES', 'password'),
        dbname=get_config('POSTGRES', 'database'),
        port=get_config('POSTGRES', 'port')
    )


def load_report_matrix():
    conn = postgres_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT customer_account, report_name, identifier FROM {get_config('POSTGRES', 'schema')}.account_reports WHERE status = 1")

    report_matrix = {}
    for customer_account, report_name, identifier in cursor.fetchall():
        report_matrix.setdefault(customer_account, {})[report_name] = identifier

    cursor.close()
    conn.close()
    return report_matrix


def fetch_reports_to_csv():
    base_url = get_config('API', 'report_api_base_url')
    username = get_config('API', 'username')
    password = get_config('API', 'password')
    customers = ast.literal_eval(get_config('CUSTOMERS', 'accounts'))

    csv_dir = 'csv_files'
    os.makedirs(csv_dir, exist_ok=True)

    report_matrix = load_report_matrix()
    all_report_names = set()
    for customer in customers:
        all_report_names.update(report_matrix.get(customer, {}).keys())

    for report_name in all_report_names:
        all_rows = []
        headers_with_customer = None

        for customer_id in customers:
            report_id = report_matrix.get(customer_id, {}).get(report_name)
            if not report_id:
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
                                    decoded = io.TextIOWrapper(csv_file, encoding='utf-8')
                                    csv_reader = csv.reader(decoded)

                                    try:
                                        headers = [h.strip() for h in next(csv_reader)]
                                    except StopIteration:
                                        continue

                                    if headers_with_customer is None:
                                        headers_with_customer = ['customer_account'] + headers

                                    for row in csv_reader:
                                        row_values = [v.strip() if v.strip() else None for v in row]
                                        while len(row_values) < len(headers):
                                            row_values.append(None)
                                        all_rows.append([customer_id] + row_values)

        if all_rows and headers_with_customer:
            file_path = os.path.join(csv_dir, f"{report_name}.csv")
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers_with_customer)
                writer.writerows(all_rows)
            logging.info(f"CSV file written: {file_path}")


def to_snake_case(name):
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.lower().replace(' ', '_')
    name = re.sub(r'_+', '_', name)
    return name.strip('_') if name.strip('_') else "unnamed_column"


# ---------- Helpers ----------

def get_db_structure(engine, schema, table_name):
    """
    Returns a list of (column_name, data_type) for a table.
    Returns None if the table does not exist.
    """
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

    if not rows:
        return None

    return [(r.column_name, r.data_type) for r in rows]


def promote_numeric_columns(df):
    """
    Attempt to promote text columns to numeric types.
    This ensures consistent type inference across runs.
    """
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
        if non_null_original.sum() > 0 and (non_null_original <= non_null_converted).all():
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
    """
    structure = []
    for col in df.columns:
        dtype = df[col].dtype

        # Check if it's a date column (from promote_date_columns)
        is_date = False
        if len(df[col].dropna()) > 0:
            is_date = df[col].dropna().apply(lambda v: isinstance(v, date)).all()

        if is_date:
            structure.append((col, "date"))
        elif dtype == 'int64' or dtype == 'Int64':
            structure.append((col, "bigint"))
        elif dtype == 'float64':
            structure.append((col, "double precision"))
        else:
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
                for statement in sql.split(';'):
                    stmt = statement.strip()
                    if stmt:
                        # Check if statement is just comments to avoid empty query error
                        is_comment_only = True
                        for line in stmt.splitlines():
                            stripped_line = line.strip()
                            if stripped_line and not stripped_line.startswith('--'):
                                is_comment_only = False
                                break

                        if not is_comment_only:
                            conn.execute(text(stmt))

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
            continue

        df_struct = infer_df_structure(df)

        # Check if structures are exactly the same
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


# ---------- Main ETL ----------

def load_csvs_to_db():
    schema = get_config("POSTGRES", "schema")

    engine = create_engine(
        f"postgresql://{get_config('POSTGRES', 'user')}:"
        f"{get_config('POSTGRES', 'password')}@"
        f"{get_config('POSTGRES', 'host')}:"
        f"{get_config('POSTGRES', 'port')}/"
        f"{get_config('POSTGRES', 'database')}"
    )

    csv_files = glob.glob("csv_files/*.csv")
    tables = {}

    print("\n" + "=" * 80)
    print("EXTRACT & TRANSFORM PHASE")
    print("=" * 80 + "\n")

    # ---------- Extract + Transform (NO DB TOUCH) ----------
    for csv_file in csv_files:
        table_name = to_snake_case(os.path.splitext(os.path.basename(csv_file))[0])

        df = pd.read_csv(csv_file, low_memory=False)
        df.columns = [to_snake_case(c) for c in df.columns]

        # Promote types in correct order
        df = promote_numeric_columns(df)
        df = promote_date_columns(df)

        # Replace full-null columns
        for col in df.columns:
            if df[col].isna().all():
                df[col] = ""

        tables[table_name] = df
        print(f"✓ Loaded CSV: {csv_file} ({len(df)} rows, {len(df.columns)} columns)")

    print("\n" + "=" * 80)
    print("VALIDATION PHASE")
    print("=" * 80 + "\n")

    # ---------- Pre-flight schema validation (STRICT) ----------
    validate_all_tables(engine, schema, tables)

    print("\n" + "=" * 80)
    print("LOAD PHASE")
    print("=" * 80 + "\n")

    # ---------- Load (safe) ----------
    for table_name, df in tables.items():
        truncate_table(engine, schema, table_name)
        df.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists="append",
            index=False
        )
        print(f"✓ Loaded {schema}.{table_name} ({len(df)} rows)")

    run_sql_files(engine, schema)

    print("\n" + "=" * 80)
    print("ETL COMPLETE")
    print("=" * 80 + "\n")


def main():
    print("\n" + "=" * 80)
    print("STARTING ETL PIPELINE")
    print("=" * 80 + "\n")

    # Uncomment to fetch reports
    # fetch_reports_to_csv()

    load_csvs_to_db()


if __name__ == "__main__":
    main()