"""Collect Doubao answers via Volcengine API with web search enabled."""

from __future__ import annotations

import logging
import sys
import time
import uuid
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from openai import OpenAI

from shared.config import CONFIG
from shared.db import execute, execute_returning, fetch_all, fetch_one, get_cursor
from shared.sql_builder import sb
from integration.citation_parser import parse_citations

logger = logging.getLogger(__name__)

_vc = CONFIG["volcengine"]

SYSTEM_PROMPT = (
    "你是一个智能搜索助手，请根据用户的搜索query给出全面、准确的回答。"
    "回答时请引用相关来源，标注引用编号。"
)

QUERY_INTERVAL_SECONDS = 180  # 每条 query 间隔 3 分钟，防止触发频率限制


class DoubaoQueryCollector:
    """Fetches Doubao-style answers from Volcengine API with web search."""

    def __init__(self):
        self.client = OpenAI(
            api_key=_vc["api_key"],
            base_url=_vc["base_url"],
        )
        self.model = _vc["seed_model"]

    # ------------------------------------------------------------------
    # Single query
    # ------------------------------------------------------------------
    def collect_answer(self, query_id: str, query_text: str) -> dict:
        """Call Volcengine API, parse answer + citations, persist to DB.

        Returns dict with keys: answer_text, citations_count, link_ids.
        """
        logger.info("Collecting answer for %s: %s", query_id, query_text[:60])

        row = execute_returning(
            "UPDATE qa_query SET status = 'processing' "
            "WHERE query_id = %s AND status = 'pending' "
            + sb.returning_clause(["updated_at"]),
            (query_id,),
            returning_select="SELECT updated_at FROM qa_query WHERE query_id = %s AND status = 'processing'",
            returning_params=(query_id,),
        )
        if not row:
            logger.info("Query %s already claimed or done, skipping", query_id)
            return {"answer_text": "", "citations_count": 0, "link_ids": [], "skipped": True}
        query_updated_at = row["updated_at"]
        existing_answer = fetch_one(
            "SELECT id FROM qa_answer WHERE query_id = %s",
            (query_id,),
        )
        if existing_answer:
            execute(
                "UPDATE qa_answer SET status = 'processing' WHERE query_id = %s",
                (query_id,),
            )
        else:
            execute(
                "INSERT INTO qa_answer (query_id, status) VALUES (%s, 'processing')",
                (query_id,),
            )

        try:
            response = self._call_api(query_text)
        except Exception as exc:
            logger.error("API call failed for %s: %s", query_id, exc)
            ol = " AND updated_at = %s" if query_updated_at else ""
            params = (str(exc)[:500], query_id, query_updated_at) if query_updated_at else (str(exc)[:500], query_id)
            n = execute(
                "UPDATE qa_query SET status = 'error', error_message = %s, "
                "retry_count = COALESCE(retry_count, 0) + 1 WHERE query_id = %s" + ol,
                params,
            )
            if query_updated_at and n == 0:
                logger.warning("Query %s optimistic lock failed (error)", query_id)
            execute(
                "UPDATE qa_answer SET status = 'error' WHERE query_id = %s",
                (query_id,),
            )
            raise

        answer_text = response.choices[0].message.content or ""
        citations = parse_citations(response)

        # Persist answer
        self._save_answer(query_id, answer_text, citations, response)

        # Persist citation links
        link_ids = self._save_links(query_id, citations)

        # Mark query done
        ol = " AND updated_at = %s" if query_updated_at else ""
        params = (query_id, query_updated_at) if query_updated_at else (query_id,)
        n = execute(
            "UPDATE qa_query SET status = 'done' WHERE query_id = %s" + ol,
            params,
        )
        if query_updated_at and n == 0:
            logger.warning("Query %s optimistic lock failed (done)", query_id)

        logger.info(
            "Done %s — answer %d chars, %d citations",
            query_id,
            len(answer_text),
            len(citations),
        )

        return {
            "answer_text": answer_text,
            "citations_count": len(citations),
            "link_ids": link_ids,
        }

    # ------------------------------------------------------------------
    # Batch processing
    # ------------------------------------------------------------------
    def batch_collect(self, batch_size: int = 10) -> list[str]:
        """Process a batch of pending queries. Returns list of processed query_ids."""
        from shared.claim_functions import claim_pending_queries
        rows = claim_pending_queries(batch_size)
        if not rows:
            logger.info("No pending queries to process.")
            return []

        processed: list[str] = []
        for i, row in enumerate(rows):
            try:
                self.collect_answer(row["query_id"], row["query_text"])
                processed.append(row["query_id"])
            except Exception:
                logger.exception("Failed to collect %s", row["query_id"])

            # 非最后一条时等待间隔，防止频率过高
            if i < len(rows) - 1:
                logger.info(
                    "Progress %d/%d — waiting %ds before next query...",
                    i + 1, len(rows), QUERY_INTERVAL_SECONDS,
                )
                time.sleep(QUERY_INTERVAL_SECONDS)

        logger.info("Batch done: %d / %d succeeded", len(processed), len(rows))
        return processed

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _call_api(self, query_text: str):
        """Invoke Volcengine chat completion, attempting web_search if available."""
        try:
            return self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": query_text},
                ],
                tools=[{"type": "web_search", "web_search": {"enable": True}}],
                temperature=0.7,
            )
        except Exception:
            # Fallback: endpoint may not support the web_search tool type
            logger.debug("web_search tool not available, falling back to basic call")
            return self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": query_text},
                ],
                temperature=0.7,
            )

    def _save_answer(
        self,
        query_id: str,
        answer_text: str,
        citations: list[dict],
        raw_response,
    ):
        """Insert or update qa_answer row.

        Guard against citation downgrade:
        if API parsing returns 0 citations but qa_link already has links for this
        query, keep has_citation/citation_count aligned with existing links.
        """
        import json

        raw_data = None
        try:
            raw_data = json.loads(raw_response.model_dump_json())
        except Exception:
            pass

        link_count_row = fetch_one(
            "SELECT count(*) AS cnt FROM qa_link WHERE query_id = %s",
            (query_id,),
        ) or {"cnt": 0}
        existing_link_count = int(link_count_row.get("cnt") or 0)
        parsed_citation_count = len(citations)
        effective_citation_count = max(parsed_citation_count, existing_link_count)
        effective_has_citation = effective_citation_count > 0

        existing = fetch_one(
            "SELECT id FROM qa_answer WHERE query_id = %s", (query_id,)
        )
        if existing:
            execute(
                "UPDATE qa_answer SET answer_text=%s, answer_length=%s, "
                "status=%s, has_citation=%s, citation_count=%s, raw_data=%s "
                "WHERE query_id=%s",
                (
                    answer_text,
                    len(answer_text),
                    "done",
                    effective_has_citation,
                    effective_citation_count,
                    json.dumps(raw_data) if raw_data else None,
                    query_id,
                ),
            )
        else:
            execute(
                "INSERT INTO qa_answer "
                "(query_id, answer_text, answer_length, status, has_citation, citation_count, raw_data) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    query_id,
                    answer_text,
                    len(answer_text),
                    "done",
                    effective_has_citation,
                    effective_citation_count,
                    json.dumps(raw_data) if raw_data else None,
                ),
            )

    def _save_links(self, query_id: str, citations: list[dict]) -> list[str]:
        """Insert qa_link rows for each citation. Returns generated link_ids."""
        link_ids: list[str] = []
        for i, cite in enumerate(citations, 1):
            if cite.get("content_format") == "商品页":
                logger.debug("Skipping 商品页 link: %s", cite["url"])

            link_id = f"{query_id}_L{i:03d}"

            old = fetch_one(
                "SELECT link_url, platform, content_format FROM qa_link WHERE link_id = %s",
                (link_id,),
            )
            _upsert_suffix = sb.upsert_suffix(
                ["link_id"],
                ["query_id", "link_url", "platform", "content_format"],
            )
            execute(
                "INSERT INTO qa_link "
                "(query_id, link_id, link_url, platform, content_format, status) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                + _upsert_suffix
                + ", publish_time=NULL, popularity=NULL, fetched_at=NULL, "
                "status='pending', updated_at=CURRENT_TIMESTAMP",
                (
                    query_id,
                    link_id,
                    cite["url"],
                    cite["platform"],
                    cite["content_format"],
                    "pending",
                ),
            )
            if old and (
                (old.get("link_url") or "").strip() != (cite["url"] or "").strip()
                or (old.get("platform") or "").strip() != (cite["platform"] or "").strip()
                or (old.get("content_format") or "").strip() != (cite["content_format"] or "").strip()
            ):
                execute("DELETE FROM qa_link_content WHERE link_id = %s", (link_id,))
            link_ids.append(link_id)

        # Clean stale links for this query when citation count shrinks.
        not_frag, not_params = sb.expand_not_all("link_id", link_ids or [""])
        stale_rows = fetch_all(
            f"SELECT link_id FROM qa_link WHERE query_id = %s AND {not_frag}",
            (query_id, *not_params),
        )
        for row in stale_rows:
            stale_id = row["link_id"]
            execute("DELETE FROM qa_link_content WHERE link_id = %s", (stale_id,))
            execute("DELETE FROM qa_link WHERE link_id = %s", (stale_id,))
        return link_ids


# ------------------------------------------------------------------
# CLI quick-test
# ------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    collector = DoubaoQueryCollector()

    if len(sys.argv) > 1 and sys.argv[1] == "--batch":
        size = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        collector.batch_collect(size)
    else:
        # Single test: grab the first pending query
        row = fetch_one(
            "SELECT query_id, query_text FROM qa_query WHERE status='pending' ORDER BY id LIMIT 1"
        )
        if row:
            result = collector.collect_answer(row["query_id"], row["query_text"])
            print(f"Answer length: {result['answer_text'][:200]}...")
            print(f"Citations: {result['citations_count']}")
            print(f"Link IDs: {result['link_ids']}")
        else:
            print("No pending queries found.")
