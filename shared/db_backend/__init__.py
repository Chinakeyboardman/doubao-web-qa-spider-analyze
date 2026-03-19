"""Database backend factory.

Usage::

    from shared.db_backend import get_backend
    backend = get_backend()          # returns singleton
    conn = backend.get_connection()
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from shared.db_backend.base import DBBackend

_backend_instance: "DBBackend | None" = None


def get_backend() -> "DBBackend":
    """Return the singleton backend matching ``CONFIG['db_type']``."""
    global _backend_instance
    if _backend_instance is not None:
        return _backend_instance

    from shared.config import CONFIG

    db_type = CONFIG["db_type"]
    if db_type == "postgresql":
        from shared.db_backend.postgresql import PostgreSQLBackend
        _backend_instance = PostgreSQLBackend()
    elif db_type == "mysql":
        from shared.db_backend.mysql import MySQLBackend
        _backend_instance = MySQLBackend()
    else:
        raise ValueError(f"Unsupported DB_TYPE: {db_type!r}. Use 'postgresql' or 'mysql'.")
    return _backend_instance


def get_dialect() -> str:
    """Shortcut returning ``'postgresql'`` or ``'mysql'``."""
    return get_backend().dialect()
