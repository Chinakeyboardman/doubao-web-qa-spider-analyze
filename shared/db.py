"""PostgreSQL connection helper with simple connection pooling."""

import contextlib
from typing import Any

import psycopg2
import psycopg2.extras

from shared.config import CONFIG

_pg = CONFIG["pg"]


def get_connection():
    """Return a new psycopg2 connection using .env settings."""
    return psycopg2.connect(
        host=_pg["host"],
        port=_pg["port"],
        dbname=_pg["dbname"],
        user=_pg["user"],
        password=_pg["password"],
    )


@contextlib.contextmanager
def get_cursor(commit: bool = True):
    """Context manager that yields a *dict* cursor and optionally commits."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
            if commit:
                conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute(sql: str, params: tuple | list | dict | None = None, *, commit: bool = True):
    """Execute a single statement (INSERT / UPDATE / DDL)."""
    with get_cursor(commit=commit) as cur:
        cur.execute(sql, params)
        return cur.rowcount


def fetch_all(sql: str, params: tuple | list | dict | None = None) -> list[dict[str, Any]]:
    """Execute a SELECT and return all rows as list of dicts."""
    with get_cursor(commit=False) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_one(sql: str, params: tuple | list | dict | None = None) -> dict[str, Any] | None:
    """Execute a SELECT and return the first row or None."""
    with get_cursor(commit=False) as cur:
        cur.execute(sql, params)
        return cur.fetchone()
