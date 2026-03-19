"""Cross-dialect SQL helpers.

Every function inspects the current ``DB_TYPE`` and returns SQL fragments
that work on both PostgreSQL and MySQL.

Usage::

    from shared.sql_builder import sb   # module-level singleton
    sql, params = sb.expand_any("link_id", id_list)
"""

from __future__ import annotations

from typing import Any, Sequence


class SQLBuilder:
    """Generates dialect-specific SQL fragments."""

    def __init__(self, dialect: str):
        self._dialect = dialect

    @property
    def dialect(self) -> str:
        return self._dialect

    @property
    def is_pg(self) -> bool:
        return self._dialect == "postgresql"

    @property
    def is_mysql(self) -> bool:
        return self._dialect == "mysql"

    # ------------------------------------------------------------------
    # Array / IN expansion
    # ------------------------------------------------------------------

    def expand_any(self, col: str, values: Sequence) -> tuple[str, list]:
        """``col = ANY(%s)`` (PG) / ``col IN (%s,%s,...)`` (MySQL).

        Returns ``(sql_fragment, param_list)``.
        """
        if self.is_pg:
            return f"{col} = ANY(%s)", [list(values)]
        placeholders = ",".join(["%s"] * len(values))
        return f"{col} IN ({placeholders})", list(values)

    def expand_not_all(self, col: str, values: Sequence) -> tuple[str, list]:
        """``col <> ALL(%s)`` (PG) / ``col NOT IN (%s,%s,...)`` (MySQL)."""
        if self.is_pg:
            return f"{col} <> ALL(%s)", [list(values)]
        placeholders = ",".join(["%s"] * len(values))
        return f"{col} NOT IN ({placeholders})", list(values)

    # ------------------------------------------------------------------
    # UPSERT
    # ------------------------------------------------------------------

    def upsert_suffix(
        self,
        conflict_cols: Sequence[str],
        update_cols: Sequence[str],
    ) -> str:
        """Return the ``ON CONFLICT ... DO UPDATE`` / ``ON DUPLICATE KEY UPDATE`` clause.

        *conflict_cols*: columns that form the unique constraint.
        *update_cols*:   columns to update on conflict.
        """
        if self.is_pg:
            sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
            keys = ", ".join(conflict_cols)
            return f"ON CONFLICT ({keys}) DO UPDATE SET {sets}"
        sets = ", ".join(f"{c} = VALUES({c})" for c in update_cols)
        return f"ON DUPLICATE KEY UPDATE {sets}"

    def upsert_do_nothing(self, conflict_cols: Sequence[str]) -> str:
        """``ON CONFLICT (k) DO NOTHING`` / ``INSERT IGNORE`` marker.

        For MySQL the caller should prepend ``INSERT IGNORE`` instead of
        ``INSERT``, so this helper returns only the PG conflict clause.
        On MySQL it returns an empty string (caller must use INSERT IGNORE).
        """
        if self.is_pg:
            keys = ", ".join(conflict_cols)
            return f"ON CONFLICT ({keys}) DO NOTHING"
        return ""

    def insert_ignore_prefix(self) -> str:
        """Return ``INSERT`` or ``INSERT IGNORE`` depending on dialect."""
        return "INSERT IGNORE" if self.is_mysql else "INSERT"

    # ------------------------------------------------------------------
    # RETURNING
    # ------------------------------------------------------------------

    def returning_clause(self, cols: Sequence[str]) -> str:
        """``RETURNING col1, col2`` on PG; empty string on MySQL."""
        if self.is_pg:
            return "RETURNING " + ", ".join(cols)
        return ""

    # ------------------------------------------------------------------
    # INTERVAL
    # ------------------------------------------------------------------

    def interval_ago(self, hours: int) -> str:
        """``CURRENT_TIMESTAMP - INTERVAL 'Nh'`` / ``DATE_SUB(NOW(), INTERVAL N HOUR)``."""
        if self.is_pg:
            return f"CURRENT_TIMESTAMP - INTERVAL '{hours} hours'"
        return f"DATE_SUB(NOW(), INTERVAL {hours} HOUR)"

    # ------------------------------------------------------------------
    # COUNT(*) FILTER
    # ------------------------------------------------------------------

    def count_filter(self, condition: str) -> str:
        """``COUNT(*) FILTER (WHERE cond)`` / ``SUM(CASE WHEN cond THEN 1 ELSE 0 END)``."""
        if self.is_pg:
            return f"COUNT(*) FILTER (WHERE {condition})"
        return f"SUM(CASE WHEN {condition} THEN 1 ELSE 0 END)"

    # ------------------------------------------------------------------
    # JSON helpers
    # ------------------------------------------------------------------

    def json_extract_text(self, col: str, key: str) -> str:
        """``col->>'key'`` / ``JSON_UNQUOTE(JSON_EXTRACT(col, '$.key'))``."""
        if self.is_pg:
            return f"{col}->>'{key}'"
        return f"JSON_UNQUOTE(JSON_EXTRACT({col}, '$.{key}'))"

    def json_extract(self, col: str, key: str) -> str:
        """``col->'key'`` / ``JSON_EXTRACT(col, '$.key')``."""
        if self.is_pg:
            return f"{col}->'{key}'"
        return f"JSON_EXTRACT({col}, '$.{key}')"

    def json_extract_path_text(self, col: str, *keys: str) -> str:
        """Deep path extraction: ``col->'a'->>'b'`` / ``JSON_UNQUOTE(JSON_EXTRACT(col, '$.a.b'))``."""
        if self.is_pg:
            parts = "->".join(f"'{k}'" for k in keys[:-1])
            if parts:
                return f"{col}->{parts}->>'{keys[-1]}'"
            return f"{col}->>'{keys[-1]}'"
        path = ".".join(keys)
        return f"JSON_UNQUOTE(JSON_EXTRACT({col}, '$.{path}'))"

    def json_extract_path(self, col: str, *keys: str) -> str:
        """Deep path extraction without unquote."""
        if self.is_pg:
            parts = "->".join(f"'{k}'" for k in keys)
            return f"{col}->{parts}"
        path = ".".join(keys)
        return f"JSON_EXTRACT({col}, '$.{path}')"

    def json_array_length(self, expr: str) -> str:
        """``jsonb_array_length(expr)`` / ``JSON_LENGTH(expr)``."""
        if self.is_pg:
            return f"jsonb_array_length({expr})"
        return f"JSON_LENGTH({expr})"

    def json_key_exists(self, col: str, key: str) -> str:
        """``col ? 'key'`` / ``JSON_CONTAINS_PATH(col, 'one', '$.key')``."""
        if self.is_pg:
            return f"{col} ? '{key}'"
        return f"JSON_CONTAINS_PATH({col}, 'one', '$.{key}')"

    def json_cast(self, literal: str) -> str:
        """``'[]'::jsonb`` / ``CAST('[]' AS JSON)``."""
        if self.is_pg:
            return f"'{literal}'::jsonb"
        return f"CAST('{literal}' AS JSON)"

    # ------------------------------------------------------------------
    # Type casts
    # ------------------------------------------------------------------

    def cast_int(self, expr: str) -> str:
        """``(expr)::INTEGER`` / ``CAST(expr AS SIGNED)``."""
        if self.is_pg:
            return f"({expr})::INTEGER"
        return f"CAST({expr} AS SIGNED)"

    # ------------------------------------------------------------------
    # GREATEST / COALESCE (both supported, but for completeness)
    # ------------------------------------------------------------------

    @staticmethod
    def coalesce(*args: str) -> str:
        """COALESCE works on both dialects."""
        return f"COALESCE({', '.join(args)})"

    @staticmethod
    def greatest(*args: str) -> str:
        """GREATEST works on both dialects."""
        return f"GREATEST({', '.join(args)})"

    # ------------------------------------------------------------------
    # FOR UPDATE SKIP LOCKED (supported on both PG and MySQL 8.0+)
    # ------------------------------------------------------------------

    @staticmethod
    def for_update_skip_locked(of_table: str | None = None) -> str:
        """Both PG and MySQL 8.0+ support this. PG allows ``FOR UPDATE OF alias``."""
        if of_table:
            return f"FOR UPDATE OF {of_table} SKIP LOCKED"
        return "FOR UPDATE SKIP LOCKED"

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def current_timestamp(self) -> str:
        return "CURRENT_TIMESTAMP"

    def now(self) -> str:
        if self.is_pg:
            return "CURRENT_TIMESTAMP"
        return "NOW()"


# ---------------------------------------------------------------------------
# Module-level singleton – import as ``from shared.sql_builder import sb``
# ---------------------------------------------------------------------------

def _make_sb() -> SQLBuilder:
    from shared.db_backend import get_dialect
    return SQLBuilder(get_dialect())


sb: SQLBuilder = _make_sb()
