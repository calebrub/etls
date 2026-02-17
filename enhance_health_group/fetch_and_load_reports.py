import os
import io
import zipfile
import base64
from datetime import date

import requests
import logging
import xml.etree.ElementTree as ET
import psycopg2
import csv
import pandas as pd
import re
from sqlalchemy import create_engine, text
import glob
from config_loader import ConfigLoader
from enhance_health_group.convert_vantage_to_enhance import convert_vantage_to_enhance

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


def fetch_reports_to_csv():
    """
    Fetch reports for all instances and write to CSV files.
    Creates separate CSV files per instance if multiple instances exist.
    """
    instances = config_loader.get_instances()
    instance_list = config_loader.list_instances()
    schema = postgres_config['schema']

    print(f"\n{'=' * 80}")
    print(f"FETCH REPORTS TO CSV - MULTI-INSTANCE MODE")
    print(f"{'=' * 80}")
    print(f"Processing {len(instance_list)} instance(s): {', '.join(instance_list)}\n")

    # Check if account_reports table has instance_key column for data isolation
    conn = postgres_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = 'account_reports' AND column_name = 'instance_key'
    """, (schema,))
    has_instance_column = cursor.fetchone() is not None
    cursor.close()
    conn.close()

    csv_dir = 'csv_files'
    os.makedirs(csv_dir, exist_ok=True)

    for instance_key in instance_list:
        instance_config = instances[instance_key]
        print(f"\n{'-' * 80}")
        print(f"INSTANCE: {instance_key}")
        print(f"{'-' * 80}")

        base_url = instance_config['api_base_url']
        username = instance_config['username']
        password = instance_config['password']
        customers = instance_config['accounts']

        # Load reports for this instance
        report_matrix = load_report_matrix(instance_key if has_instance_column else None)

        if not report_matrix:
            print(f"No reports found for instance {instance_key}")
            continue

        all_report_names = set()
        for customer in customers:
            all_report_names.update(report_matrix.get(customer, {}).keys())

        for report_name in sorted(all_report_names):
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
                                            headers_with_customer = ['customer_account', 'instance_key'] + headers

                                        for row in csv_reader:
                                            row_values = [v.strip() if v.strip() else None for v in row]
                                            while len(row_values) < len(headers):
                                                row_values.append(None)
                                            all_rows.append([customer_id, instance_key] + row_values)

            if all_rows and headers_with_customer:
                # Create subdirectory for instance if multiple instances
                if len(instance_list) > 1:
                    instance_csv_dir = os.path.join(csv_dir, instance_key)
                    os.makedirs(instance_csv_dir, exist_ok=True)
                    file_path = os.path.join(instance_csv_dir, f"{report_name}.csv")
                else:
                    file_path = os.path.join(csv_dir, f"{report_name}.csv")

                with open(file_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(headers_with_customer)
                    writer.writerows(all_rows)
                logging.info(f"CSV file written: {file_path}")
                print(f"✓ Fetched {report_name}: {len(all_rows)} rows from {len([r for r in all_rows if r[0]])}")
            else:
                print(f"⊘ No data for report: {report_name}")


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
    schema = postgres_config['schema']

    engine = create_engine(
        f"postgresql://{postgres_config['user']}:"
        f"{postgres_config['password']}@"
        f"{postgres_config['host']}:"
        f"{postgres_config['port']}/"
        f"{postgres_config['database']}"
    )

    # Get CSV files from all subdirectories (instances) or root csv_files dir
    csv_files = glob.glob("csv_files/**/*.csv", recursive=True)

    tables = {}

    print("\n" + "=" * 80)
    print("EXTRACT & TRANSFORM PHASE")
    print("=" * 80 + "\n")

    # ---------- Extract + Transform (NO DB TOUCH) ----------
    print("csv_files", csv_files)
    for csv_file in csv_files:
        # Extract table name from filename
        table_name = to_snake_case(os.path.splitext(os.path.basename(csv_file))[0])

        df = pd.read_csv(csv_file, low_memory=False)

        # Preserve instance_key column if it exists
        instance_key_col = None
        if 'instance_key' in df.columns:
            instance_key_col = df['instance_key'].copy()
            # Remove it temporarily for processing
            df = df.drop(columns=['instance_key'])

        df.columns = [to_snake_case(c) for c in df.columns]

        # Promote types in correct order
        df = promote_numeric_columns(df)
        df = promote_date_columns(df)

        # Replace full-null columns
        for col in df.columns:
            if df[col].isna().all():
                df[col] = ""

        # Add instance_key column back if it existed
        if instance_key_col is not None:
            df.insert(1, 'instance_key', instance_key_col)

        # Merge with existing dataframe if table name already exists
        if table_name in tables:
            existing_df = tables[table_name]
            existing_cols = set(existing_df.columns)
            new_cols = set(df.columns)

            # Check for column mismatches
            missing_cols = existing_cols - new_cols
            extra_cols = new_cols - existing_cols

            # Add missing columns to current df with empty strings
            if missing_cols:
                for col in missing_cols:
                    df[col] = ""
                print(f"  ⚠ Added missing columns to {csv_file}: {', '.join(sorted(missing_cols))}")

            # Remove extra columns from current df if they exist in existing but not in new
            if extra_cols:
                print(f"  ⚠ Removing extra columns from {csv_file}: {', '.join(sorted(extra_cols))}")
                df = df.drop(columns=extra_cols, errors='ignore')

            # Ensure column order matches existing dataframe
            df = df[list(existing_df.columns)]

            # Safe to merge - structures are now identical
            df = pd.concat([existing_df, df], ignore_index=True)
            print(f"✓ Merged CSV: {csv_file} ({len(df)} rows total in table '{table_name}')")
        else:
            print(f"✓ Loaded CSV: {csv_file} ({len(df)} rows, {len(df.columns)} columns)")

        tables[table_name] = df

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
        try:
            truncate_table(engine, schema, table_name)
            df.to_sql(
                table_name,
                engine,
                schema=schema,
                if_exists="append",
                index=False
            )
            print(f"✓ Loaded {schema}.{table_name} ({len(df)} rows)")
        except Exception as e:
            print(f"✗ Failed to load {schema}.{table_name}: {str(e)}")
            engine.dispose()  # Close all connections
            raise

    run_sql_files(engine, schema)

    print("\n" + "=" * 80)
    print("ETL COMPLETE")
    print("=" * 80 + "\n")


def main():
    print("\n" + "=" * 80)
    print("STARTING ETL PIPELINE")
    print("=" * 80 + "\n")

    fetch_reports_to_csv()

    load_csvs_to_db()


if __name__ == "__main__":
    main()