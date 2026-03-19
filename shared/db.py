"""Database connection helper – delegates to the configured backend (PostgreSQL / MySQL)."""

import contextlib
from typing import Any

from shared.db_backend import get_backend

_backend = get_backend()


def get_connection():
    """Return a new database connection using .env settings."""
    return _backend.get_connection()


@contextlib.contextmanager
def get_cursor(commit: bool = True):
    """Context manager that yields a *dict* cursor and optionally commits."""
    conn = _backend.get_connection()
    try:
        cur = _backend.get_dict_cursor(conn)
        try:
            yield cur
            if commit:
                conn.commit()
        finally:
            cur.close()
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


def execute_returning(
    sql: str,
    params: tuple | list | dict | None = None,
    *,
    returning_select: str | None = None,
    returning_params: tuple | list | dict | None = None,
) -> dict[str, Any] | None:
    """Execute UPDATE/INSERT and return first result row.

    On PostgreSQL the *sql* is expected to contain a ``RETURNING`` clause.
    On MySQL, pass *returning_select* – a ``SELECT`` that will be executed
    immediately after the mutation **in the same transaction** to fetch
    the equivalent row.  If *returning_select* is ``None`` on MySQL the
    function falls back to ``cur.lastrowid`` (useful for simple INSERTs).
    """
    dialect = _backend.dialect()
    conn = _backend.get_connection()
    try:
        cur = _backend.get_dict_cursor(conn)
        try:
            cur.execute(sql, params)
            if dialect == "postgresql":
                row = cur.fetchone()
            else:
                if returning_select:
                    cur.execute(returning_select, returning_params or params)
                    row = cur.fetchone()
                else:
                    row = {"last_insert_id": cur.lastrowid}
            conn.commit()
            return _backend.adapt_row(row) if row else None
        finally:
            cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetch_all(sql: str, params: tuple | list | dict | None = None) -> list[dict[str, Any]]:
    """Execute a SELECT and return all rows as list of dicts."""
    with get_cursor(commit=False) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return _backend.adapt_rows(rows)


def fetch_one(sql: str, params: tuple | list | dict | None = None) -> dict[str, Any] | None:
    """Execute a SELECT and return the first row or None."""
    with get_cursor(commit=False) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return _backend.adapt_row(row)
