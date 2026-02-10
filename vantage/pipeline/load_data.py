import time

import psycopg2
import logging
import os
from configparser import ConfigParser

# Config setup
config = ConfigParser()
config.read('config/config.ini')
schema = config['POSTGRES']['schema']

# Logging setup
os.makedirs('logs', exist_ok=True)

# PostgreSQL DB connection
def postgres_connection():
    return psycopg2.connect(
        host=config['POSTGRES']['host'],
        user=config['POSTGRES']['user'],
        password=config['POSTGRES']['password'],
        dbname=config['POSTGRES']['database'],
        port=config['POSTGRES']['port']
    )

# Run SQL from file
def run_sql(cursor, sql_path, extract_path=None, schema='public'):
    with open(sql_path, 'r') as f:
        sql_commands = f.read()

    # Replace file names with full path if needed
    if extract_path:
        sql_commands = sql_commands.replace("FROM '", f"FROM '{extract_path}/")

    # Set schema for operations
    cursor.execute(f"SET search_path TO {schema};")

    for cmd in sql_commands.split(';'):
        if cmd.strip():
            try:
                cursor.execute(cmd)
                logging.info(f"Executed SQL from {sql_path}: {cmd.strip()[:100]}...")
            except Exception as e:
                logging.error(f"Error executing SQL in {sql_path}: {cmd.strip()[:100]}... - {str(e)}")

# Check if any tables exist
def tables_exist(cursor, schema='public'):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_type = 'BASE TABLE';
    """, (schema,))
    count = cursor.fetchone()[0]
    return count > 0

def load_files_via_insert(cursor, extract_path, schema=schema):
    """Load data files into database tables using INSERT statements."""

    files_map = {
        'ar_aging': 'ar_aging.dat',
        'charges_on_hold': 'charges_on_hold.dat',
        'claim_stage_breakdown': 'claim_stage_breakdown.dat',
        'denial_trends': 'denial_trends.dat',
        'gross_billing': 'gross_billing.dat',
        'payment_trend': 'payment_trend.dat',
        'quadrant_performance': 'quadrant_performance.dat',
        'rcm_productivity': 'rcm_productivity.dat'
    }

    print(f"\n{'=' * 70}")
    print(f"Starting data load process for schema: {schema}")
    print(f"{'=' * 70}\n")

    cursor.execute(f"SET search_path TO {schema};")

    total_loaded = 0
    total_failed = 0
    start_time = time.time()

    for idx, (table, filename) in enumerate(files_map.items(), start=1):
        file_path = os.path.join(extract_path, filename)
        print(f"[{idx}/{len(files_map)}] Processing table: '{table}'")
        print(f"  File: {file_path}")

        if not os.path.exists(file_path):
            print(f"  ⚠️  WARNING: File not found - skipping table '{table}'\n")
            logging.warning(f"File not found: {file_path} (skipping table '{table}')")
            total_failed += 1
            continue

        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()

            if not lines:
                print(f"  ⚠️  WARNING: File is empty - no data to load\n")
                logging.warning(f"File is empty: {filename} (no data to load for table '{table}')")
                continue

            # Get column count from database
            cursor.execute(f"""
                SELECT count(*)
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table}';
            """)
            db_num_columns = cursor.fetchone()[0]
            print(f"  Columns in table: {db_num_columns}")

            placeholders = ', '.join(['%s'] * db_num_columns)
            insert_query = f"INSERT INTO {table} VALUES ({placeholders})"

            batch_data = []
            adjusted_rows = 0

            for line_num, line in enumerate(lines, start=1):
                row = line.strip().split('|')
                # Convert empty strings to None (NULL)
                row = [None if x == '' else x for x in row]

                # Handle column count mismatches
                if len(row) != db_num_columns:
                    adjusted_rows += 1
                    if len(row) > db_num_columns:
                        row = row[:db_num_columns]
                    else:
                        row.extend([None] * (db_num_columns - len(row)))

                batch_data.append(tuple(row))

            if adjusted_rows > 0:
                print(f"  ℹ️  Adjusted {adjusted_rows} rows with column count mismatches")

            if batch_data:
                print(f"  Inserting {len(batch_data)} rows...")
                cursor.executemany(insert_query, batch_data)
                print(f"  ✅ Successfully loaded {len(batch_data)} rows into '{table}'\n")
                logging.info(f"Loaded {len(batch_data)} rows into '{table}' from {filename}")
                total_loaded += len(batch_data)
            else:
                print(f"  ⚠️  WARNING: No valid data rows to insert\n")
                logging.warning(f"No valid data rows to insert for table '{table}'")

        except Exception as e:
            print(f"  ❌ ERROR: Failed to load table '{table}'")
            print(f"     Error type: {type(e).__name__}")
            print(f"     Error message: {e}\n")
            logging.error(f"Failed to load table '{table}' from {file_path}: {type(e).__name__}: {e}", exc_info=True)
            total_failed += 1
            raise

    elapsed_time = time.time() - start_time
    print(f"{'=' * 70}")
    print(f"Data load complete!")
    print(f"  Total rows loaded: {total_loaded:,}")
    print(f"  Tables processed: {len(files_map) - total_failed}/{len(files_map)}")
    print(f"  Tables failed: {total_failed}")
    print(f"  Time elapsed: {elapsed_time:.2f} seconds")
    print(f"{'=' * 70}\n")

    logging.info(
        f"Data load complete. Total rows: {total_loaded}, Tables failed: {total_failed}/{len(files_map)}, Time: {elapsed_time:.2f}s")

# Main loader
def load_extracted_data(extract_path, schema=schema):
    conn = postgres_connection()
    cursor = conn.cursor()

    try:
        
        if tables_exist(cursor, schema):
            logging.info('Tables already exist — skipping CREATE')
        else:
            logging.info('Tables not found — running postgres-create.sql')
            run_sql(cursor, 'sqlReportApi/psql-create.sql', schema=schema)

        dat_path =extract_path.replace("\\", "/")
        print('dat_path',dat_path)

        run_sql(cursor, 'sqlReportApi/psql-trunc.sql', schema=schema)
        
        # Use INSERT instead of COPY
        load_files_via_insert(cursor, dat_path, schema=schema)

        conn.commit()
        logging.info(f'Data Load Complete for {dat_path}')

    except Exception as e:
        conn.rollback()
        logging.error(f'Error loading data from {dat_path} - {str(e)}')

    finally:
        cursor.close()
        conn.close()
