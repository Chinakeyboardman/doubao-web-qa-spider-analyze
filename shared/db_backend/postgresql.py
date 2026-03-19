"""PostgreSQL backend using psycopg2."""

from typing import Any

import psycopg2
import psycopg2.extras

from shared.config import CONFIG
from shared.db_backend.base import DBBackend

_pg = CONFIG["pg"]


class PostgreSQLBackend(DBBackend):

    def get_connection(self) -> Any:
        return psycopg2.connect(
            host=_pg["host"],
            port=_pg["port"],
            dbname=_pg["dbname"],
            user=_pg["user"],
            password=_pg["password"],
        )

    def get_dict_cursor(self, conn: Any) -> Any:
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def get_server_side_cursor(self, conn: Any, name: str, itersize: int = 2000) -> Any:
        cur = conn.cursor(name=name, cursor_factory=psycopg2.extras.RealDictCursor)
        cur.itersize = itersize
        return cur

    def dialect(self) -> str:
        return "postgresql"
