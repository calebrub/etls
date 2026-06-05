import base64
import csv
import glob
import io
import os
import re
import xml.etree.ElementTree as ET
import zipfile

import pandas as pd
import requests
from sqlalchemy import create_engine, text

# Import local ConfigLoader
from config_loader import ConfigLoader

# Load config using multi-instance aware loader pointing to leenxa's own config
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, 'config', 'config.py')
config_loader = ConfigLoader(config_path)
postgres_config = config_loader.get_postgres_config()

def to_snake_case(name):
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.lower().replace(' ', '_')
    name = re.sub(r'_+', '_', name)
    return name.strip('_') if name.strip('_') else "unnamed_column"

def fetch_leenxa_reports_to_csv():
    """
    Fetch Leenxa reports for all instances and write to CSV files.
    """
    instances = config_loader.get_instances()
    instance_list = config_loader.list_instances()

    print(f"\n{'=' * 80}")
    print(f"FETCH LEENXA REPORTS TO CSV")
    print(f"{'=' * 80}")

    csv_dir = os.path.join(base_dir, 'csv_files')
    os.makedirs(csv_dir, exist_ok=True)

    fetched_files = []

    for instance_key in instance_list:
        instance_config = instances[instance_key]
        print(f"\nProcessing Instance: {instance_key}")

        base_url = instance_config['api_base_url']
        username = instance_config['username']
        password = instance_config['password']
        customers = instance_config['accounts']
        
        # Get reports from config for this instance
        report_configs = instance_config.get('report_configs', [])
        
        for report_cfg in report_configs:
            report_id = report_cfg['report_id']
            report_name = report_cfg['name']
            
            all_rows = []
            headers_with_customer = None
            
            for customer_id in customers:
                print(f"  → Fetching {report_name} ({report_id}) for customer: {customer_id}")
                url = f"{base_url}/customer/{customer_id}/reports/results/{report_id}"
                try:
                    response = requests.post(url, auth=(username, password), timeout=60)
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
                                                if headers_with_customer is None:
                                                    headers_with_customer = ['customer_account', 'instance_key'] + headers
                                                for row in csv_reader:
                                                    row_values = [v.strip() if v.strip() else None for v in row]
                                                    while len(row_values) < len(headers):
                                                        row_values.append(None)
                                                    all_rows.append([customer_id, instance_key] + row_values)
                                            except StopIteration:
                                                continue
                    else:
                        print(f"    × Failed (Status {response.status_code})")
                except Exception as e:
                    print(f"    × Error: {str(e)}")

            if all_rows and headers_with_customer:
                file_path = os.path.join(csv_dir, f"{to_snake_case(report_name)}.csv")
                # If file exists, append (for multi-instance)
                mode = 'a' if os.path.exists(file_path) else 'w'
                with open(file_path, mode, newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    if mode == 'w':
                        writer.writerow(headers_with_customer)
                    writer.writerows(all_rows)
                
                if file_path not in fetched_files:
                    fetched_files.append(file_path)
                print(f"  ✓ Saved {len(all_rows)} rows to {file_path}")

    return fetched_files

def promote_numeric_columns(df):
    for col in df.columns:
        if df[col].dtype != 'object' or 'date' in col.lower(): continue
        numeric_series = pd.to_numeric(df[col], errors='coerce')
        if df[col].notna().any() and numeric_series.notna().sum() == df[col].notna().sum():
            df[col] = numeric_series
    return df

def promote_date_columns(df):
    for col in df.columns:
        cleaned = df[col].replace("", pd.NA)
        if cleaned.isna().all(): continue
        parsed = pd.to_datetime(cleaned, format="%m/%d/%Y", errors="coerce")
        if not (cleaned.notna() & parsed.isna()).any():
            if "date" in col.lower() or cleaned.dropna().head(5).astype(str).str.match(r'^\d{1,2}/\d{1,2}/\d{4}$').any():
                df[col] = parsed.dt.date
    return df

def run_sql_files(engine, schema, sql_folder='sql'):
    sql_files = sorted(glob.glob(os.path.join(base_dir, sql_folder, '*.sql')))
    if not sql_files: return
    print(f"\nRunning SQL files from {sql_folder}...")
    for sql_file in sql_files:
        print(f"  Executing {os.path.basename(sql_file)}")
        with open(sql_file, 'r') as f:
            sql = f"SET search_path TO {schema};\n" + f.read()
            with engine.begin() as conn:
                for stmt in sql.split(';'):
                    if stmt.strip(): conn.execute(text(stmt))

def load_to_db(csv_paths):
    if not csv_paths: return
    schema = postgres_config['schema']
    engine = create_engine(f"postgresql://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}")
    
    for csv_path in csv_paths:
        table_name = to_snake_case(os.path.splitext(os.path.basename(csv_path))[0])
        print(f"\nLoading {table_name} into database...")
        df = pd.read_csv(csv_path, low_memory=False)
        df.columns = [to_snake_case(c) for c in df.columns]
        df = promote_numeric_columns(promote_date_columns(df))
        df['created_at'] = pd.Timestamp.now()

        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table_name}" CASCADE'))
        
        df.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)
        print(f"✓ Loaded {len(df)} rows into {schema}.{table_name}")
    
    run_sql_files(engine, schema)

def main():
    # Clear old CSVs first
    csv_dir = os.path.join(base_dir, 'csv_files')
    if os.path.exists(csv_dir):
        for f in glob.glob(os.path.join(csv_dir, "*.csv")):
            os.remove(f)

    csv_paths = fetch_leenxa_reports_to_csv()
    if csv_paths:
        load_to_db(csv_paths)
    else:
        print("No data fetched.")

if __name__ == "__main__":
    main()