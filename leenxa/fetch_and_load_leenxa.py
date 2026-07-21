import os
import sys
import logging
import psycopg2
import psycopg2.extras
from psycopg2 import sql

# Import local ConfigLoader
from config_loader import ConfigLoader

# Import logging setup from combined if available, or define local setup
try:
    from combined.logging_utils import setup_file_logging
except ImportError:
    def setup_file_logging(filename):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )

# Initialize configuration
base_dir = os.path.dirname(os.path.abspath(__file__))
config_loader = ConfigLoader()
postgres_config = config_loader.get_postgres_config()
source_db_config = config_loader.get_source_db_config()

# Setup logging
log_file = os.path.join(base_dir, "logs", "leenxa_loader.log")
os.makedirs(os.path.dirname(log_file), exist_ok=True)
setup_file_logging("leenxa_loader.log")

def get_connection(config):
    return psycopg2.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        dbname=config.get('database') or config.get('dbname'),
        port=config['port']
    )

def main():
    logging.info("Starting Leenxa ETL pipeline...")
    
    src_conn = None
    dest_conn = None
    try:
        # 1. Establish connections
        logging.info("Connecting to source database...")
        src_conn = get_connection(source_db_config)
        src_cur = src_conn.cursor()
        
        logging.info("Connecting to destination database...")
        dest_conn = get_connection(postgres_config)
        dest_cur = dest_conn.cursor()
        
        # Enable JSON/JSONB adapters on the database connections
        psycopg2.extras.register_default_jsonb(src_conn)
        psycopg2.extras.register_default_jsonb(dest_conn)
        
        # Register dictionary adapter to automatically serialize python dicts to JSONB
        from psycopg2.extensions import register_adapter
        register_adapter(dict, psycopg2.extras.Json)
        
        dest_schema = postgres_config.get('schema', 'leenxa')
        logging.info(f"Ensuring destination schema '{dest_schema}' exists...")
        dest_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {dest_schema};")
        dest_conn.commit()
        
        # Tables to replicate: (source_table_name, target_table_name, is_incremental)
        tables = [
            ("opportunities", "opportunities", True),
            ("insuranceInfo", "insuranceInfo", True),
            ("User", "User", True),
            ("LocRates", "LocRates", False) # Full sync due to small size and no updated_at
        ]
        
        for src_table, dest_table, is_incremental in tables:
            logging.info(f"Processing table: {src_table} -> {dest_schema}.{dest_table}")
            
            # Fetch source table column schema
            src_cur.execute("""
                SELECT column_name, data_type, is_nullable, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position;
            """, (src_table,))
            columns_meta = src_cur.fetchall()
            
            if not columns_meta:
                logging.error(f"Source table {src_table} has no columns or does not exist!")
                continue
                
            column_names = [col[0] for col in columns_meta]
            
            # Check if destination table exists
            dest_cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s 
                      AND table_name = %s
                );
            """, (dest_schema, dest_table))
            table_exists = dest_cur.fetchone()[0]
            
            if not table_exists:
                logging.info(f"Table {dest_schema}.{dest_table} does not exist. Creating it...")
                
                # Build DDL
                col_defs = []
                for col_name, data_type, is_nullable, max_len in columns_meta:
                    col_def = f'"{col_name}" {data_type}'
                    if data_type in ('character varying', 'varchar') and max_len:
                        col_def += f'({max_len})'
                    if is_nullable == 'NO':
                        col_def += ' NOT NULL'
                    col_defs.append(col_def)
                
                # Append primary key constraint (all these tables use id as primary key)
                col_defs.append('PRIMARY KEY ("id")')
                
                create_ddl = sql.SQL('CREATE TABLE {}.{} (\n  ' + ',\n  '.join(col_defs) + '\n);').format(
                    sql.Identifier(dest_schema),
                    sql.Identifier(dest_table)
                )
                logging.info(f"Executing DDL creation for table {dest_table}...")
                dest_cur.execute(create_ddl)
                dest_conn.commit()
            else:
                # Table exists. Check if any columns are missing in the destination table compared to source
                dest_cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s;
                """, (dest_schema, dest_table))
                dest_cols = {row[0] for row in dest_cur.fetchall()}
                
                for col_name, data_type, is_nullable, max_len in columns_meta:
                    if col_name not in dest_cols:
                        logging.info(f"Adding missing column '{col_name}' ({data_type}) to {dest_schema}.{dest_table}...")
                        col_def = f'"{col_name}" {data_type}'
                        if data_type in ('character varying', 'varchar') and max_len:
                            col_def += f'({max_len})'
                        if is_nullable == 'NO':
                            col_def += ' NOT NULL'
                        alter_query = sql.SQL('ALTER TABLE {}.{} ADD COLUMN {}').format(
                            sql.Identifier(dest_schema),
                            sql.Identifier(dest_table),
                            sql.SQL(col_def)
                        )
                        dest_cur.execute(alter_query)
                dest_conn.commit()

            # Determine incremental filter threshold
            threshold_val = None
            has_updated_at = "updatedAt" in column_names
            
            if is_incremental and has_updated_at:
                dest_cur.execute(sql.SQL('SELECT MAX("updatedAt") FROM {}.{}').format(
                    sql.Identifier(dest_schema),
                    sql.Identifier(dest_table)
                ))
                threshold_val = dest_cur.fetchone()[0]
                
            # Fetch data from source
            if threshold_val is not None:
                logging.info(f"Performing incremental sync for {src_table} since {threshold_val}...")
                # Fetch query
                query = sql.SQL('SELECT {} FROM {} WHERE "updatedAt" >= %s').format(
                    sql.SQL(', ').join(map(sql.Identifier, column_names)),
                    sql.Identifier(src_table)
                )
                src_cur.execute(query, (threshold_val,))
            else:
                logging.info(f"Performing full historical sync for {src_table}...")
                if not is_incremental:
                    # Truncate first to avoid duplicates for LocRates
                    logging.info(f"Truncating destination table {dest_schema}.{dest_table} before loading...")
                    dest_cur.execute(sql.SQL('TRUNCATE TABLE {}.{} CASCADE;').format(
                        sql.Identifier(dest_schema),
                        sql.Identifier(dest_table)
                    ))
                    dest_conn.commit()
                    
                query = sql.SQL('SELECT {} FROM {}').format(
                    sql.SQL(', ').join(map(sql.Identifier, column_names)),
                    sql.Identifier(src_table)
                )
                src_cur.execute(query)
                
            # Fetch and upsert in batches
            batch_size = 500
            total_loaded = 0
            
            # Prepare upsert query
            placeholders = sql.SQL(', ').join([sql.Placeholder()] * len(column_names))
            
            if is_incremental:
                # ON CONFLICT DO UPDATE clause
                update_cols = [c for c in column_names if c != 'id']
                update_assignments = sql.SQL(', ').join([
                    sql.SQL('{} = EXCLUDED.{}').format(sql.Identifier(c), sql.Identifier(c))
                    for c in update_cols
                ])
                
                upsert_query = sql.SQL('INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ("id") DO UPDATE SET {}').format(
                    sql.Identifier(dest_schema),
                    sql.Identifier(dest_table),
                    sql.SQL(', ').join(map(sql.Identifier, column_names)),
                    placeholders,
                    update_assignments
                )
            else:
                # Simple insert since we truncated
                upsert_query = sql.SQL('INSERT INTO {}.{} ({}) VALUES ({})').format(
                    sql.Identifier(dest_schema),
                    sql.Identifier(dest_table),
                    sql.SQL(', ').join(map(sql.Identifier, column_names)),
                    placeholders
                )
                
            while True:
                rows = src_cur.fetchmany(batch_size)
                if not rows:
                    break
                
                # Execute upsert/insert batch
                dest_cur.executemany(upsert_query, rows)
                dest_conn.commit()
                total_loaded += len(rows)
                logging.info(f"Loaded {total_loaded} rows into {dest_schema}.{dest_table}...")
                
            logging.info(f"Sync complete for {src_table}. Total rows processed: {total_loaded}")
            
        # 5. Create / Refresh Materialized View
        sql_view_path = os.path.join(base_dir, "sql", "01_opportunities_view.sql")
        if os.path.exists(sql_view_path):
            logging.info("Executing materialized view script...")
            with open(sql_view_path, 'r') as f:
                view_sql = f.read()
                
            # First execute the creation script (with IF NOT EXISTS)
            dest_cur.execute(view_sql)
            dest_conn.commit()
            
            # Then execute refresh
            logging.info("Refreshing materialized view...")
            dest_cur.execute(sql.SQL('REFRESH MATERIALIZED VIEW {}.opportunities_view;').format(
                sql.Identifier(dest_schema)
            ))
            dest_conn.commit()
            logging.info("Materialized view refreshed successfully!")
        else:
            logging.warning(f"Materialized view script not found at {sql_view_path}!")
            
    except Exception as e:
        logging.exception("An error occurred during Leenxa ETL execution:")
        if dest_conn:
            dest_conn.rollback()
        sys.exit(1)
        
    finally:
        if src_conn:
            src_conn.close()
        if dest_conn:
            dest_conn.close()
        logging.info("Leenxa ETL pipeline finished.")

if __name__ == '__main__':
    main()
