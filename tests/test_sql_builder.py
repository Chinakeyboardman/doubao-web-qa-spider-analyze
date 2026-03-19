"""Tests for shared.sql_builder – verifies both PostgreSQL and MySQL dialect output."""

import pytest
from shared.sql_builder import SQLBuilder


@pytest.fixture
def pg():
    return SQLBuilder("postgresql")


@pytest.fixture
def my():
    return SQLBuilder("mysql")


# ------------------------------------------------------------------
# expand_any / expand_not_all
# ------------------------------------------------------------------

class TestExpandAny:
    def test_pg(self, pg):
        frag, params = pg.expand_any("col", [1, 2, 3])
        assert frag == "col = ANY(%s)"
        assert params == [[1, 2, 3]]

    def test_mysql(self, my):
        frag, params = my.expand_any("col", [1, 2, 3])
        assert frag == "col IN (%s,%s,%s)"
        assert params == [1, 2, 3]


class TestExpandNotAll:
    def test_pg(self, pg):
        frag, params = pg.expand_not_all("col", ["a", "b"])
        assert frag == "col <> ALL(%s)"
        assert params == [["a", "b"]]

    def test_mysql(self, my):
        frag, params = my.expand_not_all("col", ["a", "b"])
        assert frag == "col NOT IN (%s,%s)"
        assert params == ["a", "b"]


# ------------------------------------------------------------------
# upsert_suffix
# ------------------------------------------------------------------

class TestUpsert:
    def test_pg(self, pg):
        result = pg.upsert_suffix(["id"], ["name", "value"])
        assert "ON CONFLICT (id) DO UPDATE SET" in result
        assert "EXCLUDED.name" in result
        assert "EXCLUDED.value" in result

    def test_mysql(self, my):
        result = my.upsert_suffix(["id"], ["name", "value"])
        assert "ON DUPLICATE KEY UPDATE" in result
        assert "VALUES(name)" in result
        assert "VALUES(value)" in result

    def test_do_nothing_pg(self, pg):
        result = pg.upsert_do_nothing(["id"])
        assert "ON CONFLICT (id) DO NOTHING" in result

    def test_do_nothing_mysql(self, my):
        result = my.upsert_do_nothing(["id"])
        assert result == ""

    def test_insert_ignore(self, my):
        assert my.insert_ignore_prefix() == "INSERT IGNORE"

    def test_insert_normal(self, pg):
        assert pg.insert_ignore_prefix() == "INSERT"


# ------------------------------------------------------------------
# returning_clause
# ------------------------------------------------------------------

class TestReturning:
    def test_pg(self, pg):
        assert pg.returning_clause(["id", "name"]) == "RETURNING id, name"

    def test_mysql(self, my):
        assert my.returning_clause(["id", "name"]) == ""


# ------------------------------------------------------------------
# interval_ago
# ------------------------------------------------------------------

class TestInterval:
    def test_pg(self, pg):
        result = pg.interval_ago(2)
        assert "INTERVAL" in result
        assert "2" in result

    def test_mysql(self, my):
        result = my.interval_ago(2)
        assert "DATE_SUB" in result
        assert "2 HOUR" in result


# ------------------------------------------------------------------
# count_filter
# ------------------------------------------------------------------

class TestCountFilter:
    def test_pg(self, pg):
        result = pg.count_filter("status = 'done'")
        assert "FILTER" in result

    def test_mysql(self, my):
        result = my.count_filter("status = 'done'")
        assert "SUM(CASE WHEN" in result
        assert "THEN 1 ELSE 0 END)" in result


# ------------------------------------------------------------------
# JSON helpers
# ------------------------------------------------------------------

class TestJSON:
    def test_extract_text_pg(self, pg):
        assert pg.json_extract_text("col", "key") == "col->>'key'"

    def test_extract_text_mysql(self, my):
        assert "JSON_UNQUOTE" in my.json_extract_text("col", "key")
        assert "$.key" in my.json_extract_text("col", "key")

    def test_extract_pg(self, pg):
        assert pg.json_extract("col", "key") == "col->'key'"

    def test_extract_mysql(self, my):
        assert "JSON_EXTRACT" in my.json_extract("col", "key")

    def test_array_length_pg(self, pg):
        assert pg.json_array_length("expr") == "jsonb_array_length(expr)"

    def test_array_length_mysql(self, my):
        assert my.json_array_length("expr") == "JSON_LENGTH(expr)"

    def test_key_exists_pg(self, pg):
        assert pg.json_key_exists("col", "key") == "col ? 'key'"

    def test_key_exists_mysql(self, my):
        assert "JSON_CONTAINS_PATH" in my.json_key_exists("col", "key")

    def test_json_cast_pg(self, pg):
        assert pg.json_cast("[]") == "'[]'::jsonb"

    def test_json_cast_mysql(self, my):
        assert "CAST" in my.json_cast("[]")

    def test_deep_path_text_pg(self, pg):
        result = pg.json_extract_path_text("col", "a", "b")
        assert result == "col->'a'->>'b'"

    def test_deep_path_text_mysql(self, my):
        result = my.json_extract_path_text("col", "a", "b")
        assert "$.a.b" in result


# ------------------------------------------------------------------
# cast_int
# ------------------------------------------------------------------

class TestCast:
    def test_pg(self, pg):
        assert pg.cast_int("expr") == "(expr)::INTEGER"

    def test_mysql(self, my):
        assert my.cast_int("expr") == "CAST(expr AS SIGNED)"
