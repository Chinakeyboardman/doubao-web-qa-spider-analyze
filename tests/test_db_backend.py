"""Tests for shared.db_backend factory and MySQLBackend JSON adaptation."""

import json
from unittest.mock import patch

import pytest


class TestGetBackend:
    @patch("shared.db_backend._backend_instance", None)
    @patch("shared.config.CONFIG", {"db_type": "postgresql", "pg": {
        "host": "localhost", "port": 5432, "dbname": "test", "user": "u", "password": "p",
    }})
    def test_pg_backend(self):
        from shared.db_backend import get_backend, get_dialect
        from shared.db_backend.postgresql import PostgreSQLBackend
        import shared.db_backend
        shared.db_backend._backend_instance = None
        backend = get_backend()
        assert isinstance(backend, PostgreSQLBackend)
        assert backend.dialect() == "postgresql"

    @patch("shared.db_backend._backend_instance", None)
    @patch("shared.config.CONFIG", {"db_type": "mysql", "mysql": {
        "host": "localhost", "port": 3306, "dbname": "test", "user": "u", "password": "p",
    }})
    def test_mysql_backend(self):
        from shared.db_backend.mysql import MySQLBackend
        import shared.db_backend
        shared.db_backend._backend_instance = None
        backend = shared.db_backend.get_backend()
        assert isinstance(backend, MySQLBackend)
        assert backend.dialect() == "mysql"

    @patch("shared.db_backend._backend_instance", None)
    @patch("shared.config.CONFIG", {"db_type": "sqlite"})
    def test_unsupported(self):
        import shared.db_backend
        shared.db_backend._backend_instance = None
        with pytest.raises(ValueError, match="Unsupported DB_TYPE"):
            shared.db_backend.get_backend()


class TestMySQLAdaptRow:
    def test_json_string_converted(self):
        from shared.db_backend.mysql import MySQLBackend

        backend = MySQLBackend.__new__(MySQLBackend)
        row = {
            "id": 1,
            "raw_json": '{"title": "hello"}',
            "tags": '[1, 2, 3]',
            "name": "plain text",
            "empty": "",
            "null_val": None,
        }
        adapted = backend.adapt_row(row)
        assert adapted["raw_json"] == {"title": "hello"}
        assert adapted["tags"] == [1, 2, 3]
        assert adapted["name"] == "plain text"
        assert adapted["empty"] == ""
        assert adapted["null_val"] is None

    def test_none_row(self):
        from shared.db_backend.mysql import MySQLBackend

        backend = MySQLBackend.__new__(MySQLBackend)
        assert backend.adapt_row(None) is None

    def test_invalid_json_preserved(self):
        from shared.db_backend.mysql import MySQLBackend

        backend = MySQLBackend.__new__(MySQLBackend)
        row = {"data": "{not valid json"}
        adapted = backend.adapt_row(row)
        assert adapted["data"] == "{not valid json"
