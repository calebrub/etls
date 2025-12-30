import os
import io
import zipfile
import base64
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
    cursor.execute(f"SELECT customer_account, report_name, identifier FROM {get_config('POSTGRES', 'schema')}.account_reports WHERE status = 1")
    
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

def load_csv_to_db():
    schema = get_config('POSTGRES', 'schema')
    connection_string = f"postgresql://{get_config('POSTGRES', 'user')}:{get_config('POSTGRES', 'password')}@{get_config('POSTGRES', 'host')}:{get_config('POSTGRES', 'port')}/{get_config('POSTGRES', 'database')}?sslmode=require"
    engine = create_engine(connection_string)
    
    csv_files = glob.glob("csv_files/*.csv")
    
    for csv_file in csv_files:
        table_name = to_snake_case(os.path.splitext(os.path.basename(csv_file))[0])
        
        try:
            df = pd.read_csv(csv_file, sep=',', low_memory=False)
        except Exception as e:
            print(f"Failed to load: {csv_file} {e}")
            continue
        
        df.columns = [to_snake_case(col) for col in df.columns]
        
        for col in df.columns:
            if df[col].isna().all():
                df[col] = ""
        
        conn = postgres_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE")
            conn.commit()
        finally:
            conn.close()
        
        df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
        print(f"Created {table_name} with {len(df)} rows")
    
    # Run views SQL
    with open('sql/psql-views.sql', 'r') as f:
        sql = f.read()
    
    sql = f"SET search_path TO {schema};\n" + sql
    
    with engine.connect() as conn:
        for statement in sql.split(';'):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
        conn.commit()

def main():
    print("Starting ETL Pipeline...")
    
    # Extract and Transform
    fetch_reports_to_csv()
    
    # Load
    load_csv_to_db()
    
    print("ETL Pipeline completed successfully")

if __name__ == "__main__":
    main()