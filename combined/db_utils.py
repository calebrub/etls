"""
db_utils.py
~~~~~~~~~~~
Shared database helpers used by generate_identifiers.py and fetch_and_load_reports.py.

get_db_cursor is a context manager that:
  - Opens a connection via the provided factory
  - Yields (connection, cursor) to the caller
  - Commits on clean exit
  - Rolls back and re-raises on any exception
  - Always closes the cursor and connection

Usage::

    with get_db_cursor(postgres_connection) as (conn, cur):
        cur.execute("SELECT 1")
        row = cur.fetchone()
    # connection is committed and closed here automatically
"""

from contextlib import contextmanager
from typing import Callable

import psycopg2


@contextmanager
def get_db_cursor(conn_factory: Callable[[], psycopg2.extensions.connection]):
    """
    Context manager that opens a psycopg2 connection, yields (conn, cursor),
    commits on success, and rolls back + closes on any error.

    Args:
        conn_factory: A zero-argument callable that returns a psycopg2 connection
                      (e.g. the ``postgres_connection`` function in each script).
    """
    conn = conn_factory()
    cur = conn.cursor()
    try:
        yield conn, cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
