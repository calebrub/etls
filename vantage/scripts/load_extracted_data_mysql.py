import mysql.connector
from configparser import ConfigParser
import logging
import os
import mysql
import psycopg2

config = ConfigParser()
config.read('config/config.ini')

logging.basicConfig(filename='logs/loader.log', level=logging.INFO)

def mysql_connection():
    return mysql.connector.connect(
        host=config['MYSQL']['host'],
        user=config['MYSQL']['user'],
        password=config['MYSQL']['password'],
        database=config['MYSQL']['database'],
        allow_local_infile=True
    )
    
def postgres_connection():
    print('..........',config['POSTGRES']['password'])
    return psycopg2.connect(
        host=config['POSTGRES']['host'],
        user=config['POSTGRES']['user'],
        password=config['POSTGRES']['password'],
        dbname=config['POSTGRES']['database'],
        port=config['POSTGRES']['port']
    )
    
def run_sql(cursor, sql_path, extract_path=None):
    with open(sql_path, 'r') as f:
        sql_commands = f.read()

    # Replace file names with full path if needed
    if extract_path:
        sql_commands = sql_commands.replace("INFILE '", f"INFILE '{extract_path}/")

    for cmd in sql_commands.split(';'):
        if cmd.strip():
            try:
                cursor.execute(cmd)
                logging.info(f"Executed SQL from {sql_path}: {cmd.strip()[:100]}...")
            except Exception as e:
                logging.error(f"Error executing SQL in {sql_path}: {cmd.strip()[:100]}... - {str(e)}")

def tables_exist(cursor):
    cursor.execute('SHOW TABLES')
    tables = cursor.fetchall()
    return len(tables) > 0

def load_extracted_data_mysql(extract_path):
    conn = postgres_connection()

    cursor = conn.cursor()
    cursor.execute("SET GLOBAL local_infile = 1")  # Enables LOCAL for that session

    try:
        if tables_exist(cursor):
            logging.info('Tables already exist — skipping CREATE')
        else:
            logging.info('Tables not found — running mysql-create.sql')
            # run_sql(cursor, 'sql/mysql-drop.sql')
            run_sql(cursor, 'sql/mysql-create.sql')
        
        run_sql(cursor, 'sql/mysql-load.sql', extract_path)
        conn.commit()
        logging.info(f'Data Load Complete for {extract_path}')

    except Exception as e:
        conn.rollback()
        logging.error(f'Error loading data from {extract_path} - {str(e)}')

    finally:
        cursor.close()
        conn.close()
