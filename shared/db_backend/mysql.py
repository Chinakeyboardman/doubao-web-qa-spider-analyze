"""MySQL backend using PyMySQL."""

import json
from typing import Any

import pymysql
import pymysql.cursors

from shared.config import CONFIG
from shared.db_backend.base import DBBackend

_my = CONFIG["mysql"]

# Column types returned by PyMySQL that may contain JSON strings.
_JSON_FIELD_TYPES = frozenset()  # populated lazily if needed


class MySQLBackend(DBBackend):

    def get_connection(self) -> Any:
        return pymysql.connect(
            host=_my["host"],
            port=_my["port"],
            database=_my["dbname"],
            user=_my["user"],
            password=_my["password"],
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    def get_dict_cursor(self, conn: Any) -> Any:
        return conn.cursor(pymysql.cursors.DictCursor)

    def get_server_side_cursor(self, conn: Any, name: str, itersize: int = 2000) -> Any:
        return conn.cursor(pymysql.cursors.SSDictCursor)

    def dialect(self) -> str:
        return "mysql"

    # ------------------------------------------------------------------
    # PyMySQL returns JSON columns as plain strings; psycopg2 returns
    # JSONB as Python dicts automatically.  We detect string values that
    # look like JSON objects/arrays and parse them so that callers always
    # receive the same types regardless of backend.
    # ------------------------------------------------------------------

    def adapt_row(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        adapted = {}
        for k, v in row.items():
            if isinstance(v, str) and v and v[0] in ('{', '['):
                try:
                    adapted[k] = json.loads(v)
                except (json.JSONDecodeError, ValueError):
                    adapted[k] = v
            else:
                adapted[k] = v
        return adapted
