"""Python implementations of the claim_pending_* stored functions.

These replace the PostgreSQL plpgsql functions so that the same logic works
on both PostgreSQL and MySQL.  They use SELECT … FOR UPDATE SKIP LOCKED
(supported by MySQL 8.0+) inside an explicit transaction.
"""

from __future__ import annotations

from typing import Any

from shared.db_backend import get_backend
from shared.sql_builder import sb


def claim_pending_queries(
    limit: int,
    start_id: str | None = None,
    end_id: str | None = None,
) -> list[dict[str, Any]]:
    """Atomically claim up to *limit* pending queries.

    Returns list of dicts with keys: ``query_id``, ``query_text``, ``updated_at``.
    """
    backend = get_backend()
    conn = backend.get_connection()
    try:
        cur = backend.get_dict_cursor(conn)
        try:
            # 1. SELECT candidates FOR UPDATE SKIP LOCKED
            where_parts = ["q.status = 'pending'"]
            params: list[Any] = []
            if start_id is not None:
                where_parts.append("q.query_id >= %s")
                params.append(start_id)
            if end_id is not None:
                where_parts.append("q.query_id <= %s")
                params.append(end_id)

            where_clause = " AND ".join(where_parts)
            select_sql = (
                f"SELECT q.query_id FROM qa_query q "
                f"WHERE {where_clause} "
                f"ORDER BY q.id LIMIT %s "
                f"FOR UPDATE SKIP LOCKED"
            )
            params.append(limit)
            cur.execute(select_sql, params)
            rows = cur.fetchall()
            if not rows:
                conn.commit()
                return []

            ids = [r["query_id"] for r in rows]

            # 2. UPDATE claimed rows
            any_frag, any_params = sb.expand_any("query_id", ids)
            update_sql = (
                f"UPDATE qa_query SET status = 'processing', "
                f"updated_at = CURRENT_TIMESTAMP "
                f"WHERE {any_frag}"
            )
            cur.execute(update_sql, any_params)

            # 3. SELECT the updated rows to return
            select_back = (
                f"SELECT query_id, query_text, updated_at "
                f"FROM qa_query WHERE {any_frag}"
            )
            cur.execute(select_back, any_params)
            result = cur.fetchall()
            conn.commit()
            return backend.adapt_rows(result)
        finally:
            cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def claim_pending_links(
    limit: int,
    query_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Atomically claim up to *limit* pending links.

    Returns list of dicts with keys:
    ``link_id``, ``link_url``, ``platform``, ``content_format``, ``updated_at``.
    """
    backend = get_backend()
    conn = backend.get_connection()
    try:
        cur = backend.get_dict_cursor(conn)
        try:
            where_parts = ["l.status = 'pending'"]
            params: list[Any] = []
            if query_ids:
                any_frag, any_params = sb.expand_any("l.query_id", query_ids)
                where_parts.append(any_frag)
                params.extend(any_params)

            where_clause = " AND ".join(where_parts)
            select_sql = (
                f"SELECT l.link_id FROM qa_link l "
                f"WHERE {where_clause} "
                f"ORDER BY l.id LIMIT %s "
                f"FOR UPDATE SKIP LOCKED"
            )
            params.append(limit)
            cur.execute(select_sql, params)
            rows = cur.fetchall()
            if not rows:
                conn.commit()
                return []

            ids = [r["link_id"] for r in rows]

            any_frag2, any_params2 = sb.expand_any("link_id", ids)
            update_sql = (
                f"UPDATE qa_link SET status = 'processing', "
                f"updated_at = CURRENT_TIMESTAMP "
                f"WHERE {any_frag2}"
            )
            cur.execute(update_sql, any_params2)

            select_back = (
                f"SELECT link_id, link_url, platform, content_format, updated_at "
                f"FROM qa_link WHERE {any_frag2}"
            )
            cur.execute(select_back, any_params2)
            result = cur.fetchall()
            conn.commit()
            return backend.adapt_rows(result)
        finally:
            cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def claim_pending_video_parse_v2(
    limit: int,
    query_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Atomically claim up to *limit* pending video parse tasks.

    Returns list of dicts with keys:
    ``vid``, ``link_id``, ``query_id``, ``link_url``, ``raw_json``,
    ``model_api_input_type``, ``video_updated_at``, ``content_updated_at``.
    """
    backend = get_backend()
    conn = backend.get_connection()
    try:
        cur = backend.get_dict_cursor(conn)
        try:
            where_parts = [
                "v.status IN ('pending', 'error')",
                "l.status = 'done'",
                "COALESCE(v.stt_text, '') = ''",
                "COALESCE(v.retry_count, 0) < 3",
                (
                    "NOT EXISTS ("
                    "  SELECT 1 FROM qa_link_video sib "
                    "  WHERE sib.link_id = v.link_id "
                    "    AND sib.model_api_input_type = 'input_audio' "
                    "    AND sib.status = 'done'"
                    ")"
                ),
            ]
            params: list[Any] = []
            if query_ids:
                any_frag, any_params = sb.expand_any("l.query_id", query_ids)
                where_parts.append(any_frag)
                params.extend(any_params)

            where_clause = " AND ".join(where_parts)
            select_sql = (
                f"SELECT v.id AS vid, v.link_id, l.query_id, l.link_url, "
                f"       lc.raw_json, v.model_api_input_type, "
                f"       lc.updated_at AS content_updated_at "
                f"FROM qa_link_video v "
                f"JOIN qa_link l ON l.link_id = v.link_id "
                f"LEFT JOIN qa_link_content lc ON lc.link_id = v.link_id "
                f"WHERE {where_clause} "
                f"ORDER BY CASE WHEN v.model_api_input_type = 'input_audio' THEN 0 ELSE 1 END, v.id "
                f"LIMIT %s "
                f"FOR UPDATE OF v SKIP LOCKED"
            )
            params.append(limit)

            # MySQL does not support FOR UPDATE OF <alias>; fall back to FOR UPDATE SKIP LOCKED
            if sb.is_mysql:
                select_sql = select_sql.replace("FOR UPDATE OF v SKIP LOCKED", "FOR UPDATE SKIP LOCKED")

            cur.execute(select_sql, params)
            rows = cur.fetchall()
            if not rows:
                conn.commit()
                return []

            vid_list = [r["vid"] for r in rows]
            any_frag2, any_params2 = sb.expand_any("id", vid_list)

            update_sql = (
                f"UPDATE qa_link_video SET status = 'processing', "
                f"updated_at = CURRENT_TIMESTAMP "
                f"WHERE {any_frag2}"
            )
            cur.execute(update_sql, any_params2)

            # Re-select to get video_updated_at
            select_back = (
                f"SELECT v.id AS vid, v.link_id, v.updated_at AS video_updated_at "
                f"FROM qa_link_video v WHERE {any_frag2}"
            )
            cur.execute(select_back, any_params2)
            updated_map = {r["vid"]: r for r in cur.fetchall()}

            conn.commit()

            result = []
            for r in backend.adapt_rows(rows):
                vid = r["vid"]
                r["video_updated_at"] = updated_map.get(vid, {}).get("video_updated_at")
                result.append(r)
            return result
        finally:
            cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
