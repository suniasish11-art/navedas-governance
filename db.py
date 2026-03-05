"""
db.py — Unified database connection for Navedas Governance Platform.

Supports:
  - SQLite  (local / Streamlit Cloud fallback, ephemeral)
  - Neon PostgreSQL (persistent cloud storage)

Set NEON_DATABASE_URL in Streamlit secrets or env to enable Neon.
Falls back to SQLite automatically if not set.
"""
import os
import re
import tempfile

SQLITE_PATH = os.path.join(tempfile.gettempdir(), 'navedas_governance.db')

# Conflict-resolution clauses for PostgreSQL upserts
_ON_CONFLICT = {
    'orders_feed': 'ON CONFLICT (order_id) DO NOTHING',
}


def _neon_url() -> str:
    """Read Neon connection string from env or Streamlit secrets."""
    url = os.environ.get("NEON_DATABASE_URL", "")
    if not url:
        try:
            import streamlit as st
            url = st.secrets.get("NEON_DATABASE_URL", "") or ""
        except Exception:
            pass
    return url


def _parse_neon(url: str) -> dict:
    """Parse Neon URL into connection kwargs for psycopg2/SQLAlchemy."""
    from urllib.parse import urlparse, unquote
    p = urlparse(url)
    return {
        'host':     p.hostname,
        'port':     p.port or 5432,
        'dbname':   p.path.lstrip('/'),
        'user':     unquote(p.username or ''),
        'password': unquote(p.password or ''),
        'sslmode':  'require',
    }


def is_neon() -> bool:
    return bool(_neon_url())


def get_engine():
    """Return a SQLAlchemy engine — used by pandas to_sql / read_sql."""
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    url = _neon_url()
    if url:
        p = _parse_neon(url)
        # Build clean URL with no query params; pass SSL via connect_args
        sa_url = (
            f"postgresql+psycopg2://{p['user']}:{p['password']}"
            f"@{p['host']}:{p['port']}/{p['dbname']}"
        )
        return create_engine(
            sa_url,
            poolclass=NullPool,
            connect_args={"sslmode": "require"},
        )
    return create_engine(f"sqlite:///{SQLITE_PATH}")


def _adapt_sql(sql: str) -> str:
    """Convert SQLite SQL dialect to PostgreSQL-compatible SQL."""
    # Named params  :foo  →  %(foo)s
    sql = re.sub(r':(\w+)', r'%(\1)s', sql)
    # Positional ?  →  %s
    sql = sql.replace('?', '%s')
    # INSERT OR IGNORE INTO  →  INSERT INTO
    sql = re.sub(r'INSERT\s+OR\s+IGNORE\s+INTO\s+',
                 'INSERT INTO ', sql, flags=re.IGNORECASE)
    # AUTOINCREMENT  →  (remove; PostgreSQL SERIAL handles this)
    sql = re.sub(r'\s*AUTOINCREMENT\b', '', sql, flags=re.IGNORECASE)
    # INTEGER PRIMARY KEY  →  SERIAL PRIMARY KEY  (CREATE TABLE only)
    sql = re.sub(r'\bINTEGER\s+PRIMARY\s+KEY\b',
                 'SERIAL PRIMARY KEY', sql, flags=re.IGNORECASE)
    # Add ON CONFLICT clause for known tables
    for tbl, conflict in _ON_CONFLICT.items():
        if (re.search(rf'INSERT\s+INTO\s+{tbl}\b', sql, re.IGNORECASE)
                and 'ON CONFLICT' not in sql.upper()):
            sql = sql.rstrip() + f' {conflict}'
    return sql


class _FakeCursor:
    """Dummy cursor returned for BEGIN/COMMIT/ROLLBACK so callers
    can still chain .fetchone() without errors."""
    def fetchone(self):  return None
    def fetchall(self):  return []


class _PgWrapper:
    """
    Wraps a psycopg2 connection with a sqlite3-compatible surface:
      .execute(sql, params)
      .executemany(sql, seq)
      .executescript(script)
      .commit()
      .close()
      .raw   →  underlying psycopg2 connection (for pandas.read_sql)
    """
    def __init__(self, conn):
        self._c = conn

    @property
    def raw(self):
        return self._c

    def execute(self, sql: str, params=()):
        sql_up = sql.strip().upper()
        if sql_up in ('BEGIN', 'BEGIN TRANSACTION'):
            return _FakeCursor()
        if sql_up == 'COMMIT':
            self._c.commit()
            return _FakeCursor()
        if sql_up == 'ROLLBACK':
            self._c.rollback()
            return _FakeCursor()
        adapted = _adapt_sql(sql)
        cur = self._c.cursor()
        cur.execute(adapted, params if params else None)
        return cur

    def executemany(self, sql: str, seq):
        import psycopg2.extras
        adapted = _adapt_sql(sql)
        cur = self._c.cursor()
        psycopg2.extras.execute_batch(cur, adapted, list(seq))
        return cur

    def executescript(self, script: str):
        cur = self._c.cursor()
        for stmt in script.split(';'):
            s = stmt.strip()
            if s:
                cur.execute(_adapt_sql(s))
        self._c.commit()

    def commit(self):
        self._c.commit()

    def close(self):
        try:
            self._c.close()
        except Exception:
            pass


def get_conn(path: str = SQLITE_PATH):
    """
    Return a database connection.
    - NEON_DATABASE_URL set  →  psycopg2-backed _PgWrapper
    - Otherwise             →  sqlite3 connection (path is used)
    """
    url = _neon_url()
    if url:
        import psycopg2
        p = _parse_neon(url)
        return _PgWrapper(psycopg2.connect(**p))
    import sqlite3
    return sqlite3.connect(path)
