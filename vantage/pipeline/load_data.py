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
        port=config['POSTGRES']['port'],
        sslmode="require"
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
        
        run_sql(cursor, 'sqlReportApi/psql-load.sql', dat_path, schema=schema)
        conn.commit()
        logging.info(f'Data Load Complete for {dat_path}')

    except Exception as e:
        conn.rollback()
        logging.error(f'Error loading data from {dat_path} - {str(e)}')

    finally:
        cursor.close()
        conn.close()
