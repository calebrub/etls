import psycopg2
import pandas as pd
import hashlib
import sys

POSTGRES = {
    'host': 'revlooppgserver.postgres.database.azure.com',
    'user': 'REVETLCUSPRODUSER',
    'password': 'uO63mP5df9KvLhVZZHdkr3cG',
    'database': 'REVETLCUSPRODDB',
    'port': '5432',
}

TABLES_TO_VIEWS = {
    "charges_on_hold": ["charges_on_hold_view"],
    "quadrant_performance": ["quadrant_performance_view"]
}

def compute_row_hash(df: pd.DataFrame, exclude_cols=None) -> pd.Series:
    if exclude_cols is None:
        exclude_cols = {'created_at', 'row_hash', 'ctid'}
    cols = [c for c in df.columns if c not in exclude_cols]
    parts = [df[c].map(str) for c in cols]
    combined = parts[0] if len(parts) == 1 else parts[0].str.cat(parts[1:], sep='|')
    return combined.map(lambda v: hashlib.md5(v.encode('utf-8')).hexdigest())

def clean_and_align_table(conn, table_name, views):
    cur = conn.cursor()
    print(f"\n================================================================================")
    print(f"PROCESSING TABLE: {table_name}")
    print(f"================================================================================")
    
    # 1. Fetch column list
    cur.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'dw_combined' 
        AND table_name = '{table_name}'
        ORDER BY ordinal_position;
    """)
    cols = [r[0] for r in cur.fetchall()]
    exclude = {'created_at', 'row_hash'}
    group_cols = [c for c in cols if c not in exclude]
    
    group_cols_str = ", ".join([f'"{c}"' for c in group_cols])
    
    # 2. Deduplicate first using SQL
    print("Deduplicating rows using partition window function...")
    delete_query = f"""
        WITH ranked AS (
            SELECT ctid,
                   row_number() OVER (
                       PARTITION BY {group_cols_str}
                       ORDER BY ctid
                   ) as rn
            FROM dw_combined."{table_name}"
        )
        DELETE FROM dw_combined."{table_name}"
        WHERE ctid IN (SELECT ctid FROM ranked WHERE rn > 1);
    """
    cur.execute(delete_query)
    deleted = cur.rowcount
    print(f"  Deleted {deleted} duplicate rows.")
    cur.close()
    
    # 3. Recompute and update hashes in chunks using a named server-side cursor
    print("Processing row hash updates in chunks...")
    conn_cursor = psycopg2.connect(**POSTGRES)
    cursor_name = f'cur_{table_name}'
    
    cols_with_ctid = cols + ['ctid']
    
    try:
        cur_server = conn_cursor.cursor(cursor_name)
        cur_server.execute(f'SELECT *, ctid FROM dw_combined."{table_name}";')
        
        chunk_idx = 0
        total_updated = 0
        
        while True:
            rows = cur_server.fetchmany(50000)
            if not rows:
                break
                
            chunk_idx += 1
            print(f"  Processing chunk {chunk_idx} (50,000 rows)...")
            
            df = pd.DataFrame(rows, columns=cols_with_ctid)
            df['new_row_hash'] = compute_row_hash(df)
            
            cur_update = conn.cursor()
            cur_update.execute("DROP TABLE IF EXISTS temp_hash_alignment;")
            cur_update.execute("""
                CREATE TEMP TABLE temp_hash_alignment (
                    row_ctid tid,
                    row_hash text
                );
            """)
            
            data = list(zip(df['ctid'], df['new_row_hash']))
            args_str = ','.join(cur_update.mogrify("(%s,%s)", x).decode('utf-8') for x in data)
            cur_update.execute("INSERT INTO temp_hash_alignment (row_ctid, row_hash) VALUES " + args_str)
            
            cur_update.execute(f"""
                UPDATE dw_combined."{table_name}" t
                SET row_hash = s.row_hash
                FROM temp_hash_alignment s
                WHERE t.ctid = s.row_ctid AND (t.row_hash IS DISTINCT FROM s.row_hash);
            """)
            updated_chunk = cur_update.rowcount
            total_updated += updated_chunk
            
            cur_update.execute("DROP TABLE IF EXISTS temp_hash_alignment;")
            cur_update.close()
            
        print(f"  Finished chunked hashing. Total row hashes updated: {total_updated}")
        cur_server.close()
    finally:
        conn_cursor.close()
        
    # 4. Refresh materialized views
    cur = conn.cursor()
    for view in views:
        print(f"Refreshing materialized view: {view}...")
        cur.execute(f'REFRESH MATERIALIZED VIEW dw_combined."{view}";')
        print(f"  Successfully refreshed {view}.")
    cur.close()
    print(f"Finished table {table_name}!")

def main():
    try:
        conn = psycopg2.connect(**POSTGRES)
        conn.autocommit = True
        
        for table, views in TABLES_TO_VIEWS.items():
            clean_and_align_table(conn, table, views)
            
        conn.close()
        print("\nAll database cleaning and hash alignments completed successfully!")
    except Exception as e:
        print("Error:", e)
        sys.exit(1)

if __name__ == '__main__':
    main()
