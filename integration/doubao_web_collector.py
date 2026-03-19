"""Doubao web collector — extracts deep-thinking links via Playwright.

Automates the real Doubao web UI (https://www.doubao.com/chat/) to:
  1. Log in via SMS verification code (using sms.guangyinai.com API)
  2. Send queries and wait for AI response
  3. Expand the "深度思考" section and extract ALL reference links
  4. Persist answers + links to PostgreSQL

Prerequisites:
  pip install playwright && playwright install chromium

Usage:
  python integration/doubao_web_collector.py login            # Auto-login via SMS
  python integration/doubao_web_collector.py login --manual    # Manual login in browser
  python integration/doubao_web_collector.py collect [--query-id Q0001]
  python integration/doubao_web_collector.py batch   [--batch-size 5]
  python integration/doubao_web_collector.py test              # One query end-to-end test
  python integration/doubao_web_collector.py debug             # Dump page HTML + screenshot
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
import time
from pathlib import Path

import httpx

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from shared.config import CONFIG
from shared.db import execute, execute_returning, fetch_all, fetch_one
from shared.sql_builder import sb
from integration.citation_parser import identify_platform, determine_content_format

logger = logging.getLogger(__name__)

DOUBAO_URL = "https://www.doubao.com/chat/"
STATE_DIR = Path(__file__).parent / ".browser_state"
QUERY_INTERVAL = 60

_URL_RE = re.compile(r"https?://[^\s\])'\"<>]+")
_sms_cfg = CONFIG["sms_api"]


class HumanVerificationRequired(RuntimeError):
    """Raised when Doubao blocks automation with a human verification challenge."""


class ChatNotReadyError(RuntimeError):
    """Raised when default chat session/input is not ready for typing."""


# ======================================================================
# SMS API helper
# ======================================================================
class SmsApi:
    """Wrapper around sms.guangyinai.com for phone number + verification code."""

    def __init__(self):
        self.base = _sms_cfg["base_url"]
        self.token = _sms_cfg["token"]
        self.device_id = _sms_cfg["device_id"]
        self.platform = _sms_cfg["platform"]

    @property
    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    async def get_phone(self, profile_id: str = "1") -> str | None:
        """Get an available phone number (returns digits without +86)."""
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(
                f"{self.base}/api/phone/get",
                params={
                    "device_id": self.device_id,
                    "platform": self.platform,
                    "profile_id": profile_id,
                },
                headers=self._headers,
            )
            data = resp.json()
            if data.get("success") and data.get("phoneNumber"):
                phone = data["phoneNumber"].replace("+86", "")
                logger.info("Got phone number: %s", phone)
                return phone
            logger.error("Failed to get phone: %s", data)
            return None

    async def get_sms_code(
        self, phone: str, profile_id: str = "1", retries: int = 5, interval: int = 5
    ) -> str | None:
        """Poll for the SMS verification code.

        The API may return JSON {"code":"821298"} or plain text "821298".
        """
        key = f"{phone}_{self.platform}"
        async with httpx.AsyncClient(timeout=10) as c:
            for attempt in range(1, retries + 1):
                resp = await c.get(
                    f"{self.base}/api/messages/latest",
                    params={
                        "key": key,
                        "deviceId": self.device_id,
                        "profileId": profile_id,
                    },
                    headers=self._headers,
                )
                if resp.status_code == 200:
                    code = self._parse_code(resp.text)
                    if code:
                        logger.info("Got SMS code: %s", code)
                        return code
                logger.info(
                    "SMS code attempt %d/%d — status %d, body=%s",
                    attempt, retries, resp.status_code, resp.text[:80],
                )
                if attempt < retries:
                    await asyncio.sleep(interval)
        logger.error("Failed to get SMS code after %d retries", retries)
        return None

    @staticmethod
    def _parse_code(text: str) -> str | None:
        """Extract 4-6 digit code from JSON or plain text response."""
        text = text.strip()
        # Try JSON first: {"code":"821298"}
        try:
            data = json.loads(text)
            if isinstance(data, dict) and "code" in data:
                code = str(data["code"]).strip()
                if re.fullmatch(r"\d{4,6}", code):
                    return code
        except (json.JSONDecodeError, TypeError):
            pass
        # Fallback: plain text
        if re.fullmatch(r"\d{4,6}", text):
            return text
        return None

    async def mark_phone_busy(self, phone: str, profile_id: str = "1"):
        """Notify the API that a phone number is rate-limited / unusable."""
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(
                f"{self.base}/api/phone/bid",
                json={
                    "deviceId": self.device_id,
                    "phoneNumber": phone,
                    "platform": self.platform,
                    "profileId": profile_id,
                },
                headers=self._headers,
            )


# ======================================================================
# DoubaoWebCollector
# ======================================================================
class DoubaoWebCollector:
    """Browser-based Doubao collector with deep-thinking link extraction."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None
        self._sms = SmsApi()
        self._last_login_phone: str = ""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        from playwright.async_api import async_playwright

        STATE_DIR.mkdir(parents=True, exist_ok=True)
        self._pw = await async_playwright().start()

        launch_opts: dict = {
            "headless": self.headless,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        self._browser = await self._pw.chromium.launch(**launch_opts)

        ctx_opts: dict = {
            "viewport": {"width": 1280, "height": 900},
            "user_agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        }
        state_file = STATE_DIR / "state.json"
        if state_file.exists():
            ctx_opts["storage_state"] = str(state_file)

        self._context = await self._browser.new_context(**ctx_opts)
        self._page = await self._context.new_page()

    async def stop(self):
        if self._browser:
            await self._browser.close()
        if self._pw:
            await self._pw.stop()
        self._browser = self._pw = None

    async def save_state(self):
        await self._context.storage_state(path=str(STATE_DIR / "state.json"))
        logger.info("Browser state saved")

    # ------------------------------------------------------------------
    # Login — automatic via SMS API
    # ------------------------------------------------------------------
    async def auto_login(self, *, force_relogin: bool = False) -> bool:
        """Full automatic login: get phone → enter → get SMS code → verify."""
        page = self._page
        if force_relogin:
            state_file = STATE_DIR / "state.json"
            if state_file.exists():
                state_file.unlink(missing_ok=True)
            logger.warning("Force relogin enabled: cleared stored browser state")
        await page.goto(DOUBAO_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        if await self._is_logged_in():
            logger.info("Already logged in (session restored)")
            await self.save_state()
            return True

        # Step 1: open login modal
        login_btn = page.locator("button:has-text('登录'), [class*='login']")
        if await login_btn.count() == 0:
            logger.error("Login button not found")
            return False
        await login_btn.first.click()
        await page.wait_for_timeout(2000)

        # Step 2: get phone from SMS API
        phone = await self._sms.get_phone()
        if not phone:
            logger.error("Could not get phone number from SMS API")
            return False
        self._last_login_phone = phone

        # Step 3: fill phone number
        phone_input = page.locator('input[placeholder="请输入手机号"]')
        await phone_input.fill(phone)
        await page.wait_for_timeout(300)

        # Step 4: check the agreement checkbox
        checkbox_display = page.locator(".semi-checkbox-inner-display")
        if await checkbox_display.count() > 0:
            await checkbox_display.first.click(force=True)
            await page.wait_for_timeout(300)

        # Step 5: click "下一步" — this triggers SMS sending
        next_btn = page.locator('button:has-text("下一步")')
        await next_btn.first.click()
        logger.info("Clicked next — SMS will be sent to %s", phone)
        await page.wait_for_timeout(2000)

        # Step 6: wait 45s then poll for SMS code
        logger.info("Waiting 45s for SMS delivery...")
        await asyncio.sleep(45)
        code = await self._sms.get_sms_code(phone)
        if not code:
            logger.error("Could not retrieve SMS code")
            await self._sms.mark_phone_busy(phone)
            return False

        # Step 7: type the 6-digit code one digit at a time
        # Doubao uses 6 separate input boxes
        logger.info("Entering verification code: %s", code)
        code_inputs = page.locator(
            '[class*="modal"] input:visible, [class*="dialog"] input:visible'
        )
        input_count = await code_inputs.count()

        if input_count >= 6:
            for i, digit in enumerate(code[:6]):
                await code_inputs.nth(i).fill(digit)
                await page.wait_for_timeout(100)
        elif input_count >= 1:
            await code_inputs.first.fill(code[:6])
        else:
            # Fallback: type via keyboard
            await page.keyboard.type(code[:6], delay=100)

        await page.wait_for_timeout(5000)

        # Step 8: 若出现验证码，先尝试自动求解
        if await self._check_captcha():
            try:
                from integration.captcha_solver import try_solve_captcha

                solved, _ = await try_solve_captcha(page)
                if solved:
                    await page.wait_for_timeout(3000)
            except Exception as e:
                logger.debug("Login captcha auto-solve: %s", e)

        # Step 9: verify login succeeded
        if await self._is_logged_in():
            await self.save_state()
            logger.info("Auto-login successful")
            return True

        logger.error("Login verification failed — page may require CAPTCHA")
        await self._sms.mark_phone_busy(phone)
        return False

    # ------------------------------------------------------------------
    # Login — manual (user logs in themselves)
    # ------------------------------------------------------------------
    async def manual_login(self, timeout: int = 300) -> bool:
        page = self._page
        await page.goto(DOUBAO_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        if await self._is_logged_in():
            logger.info("Already logged in")
            await self.save_state()
            return True

        print("\n" + "=" * 60)
        print("  请在弹出的浏览器窗口中手动登录豆包")
        print("  登录完成后脚本会自动检测并保存登录状态")
        print("=" * 60 + "\n")

        deadline = time.time() + timeout
        while time.time() < deadline:
            if await self._is_logged_in():
                await self.save_state()
                print("\n登录成功，状态已保存！\n")
                return True
            await asyncio.sleep(3)

        print("\n登录超时。\n")
        return False

    async def ensure_logged_in(self) -> bool:
        """确保已登录：有 state 则复用，否则走 SMS 模拟登录；登录状态会保存，可重复用。"""
        # auto_login 内部会先检测已登录则直接返回并 save_state
        ok = await self.auto_login()
        if ok:
            logger.info("Login state will be reused in next run")
        return ok

    async def switch_account(self) -> bool:
        """Force switch account by dropping state and requesting a new phone login."""
        if self._last_login_phone:
            try:
                await self._sms.mark_phone_busy(self._last_login_phone)
                logger.info("Marked phone as busy for account switch: %s", self._last_login_phone)
            except Exception as exc:
                logger.warning("Failed to mark old phone busy during account switch: %s", exc)
        return await self.auto_login(force_relogin=True)

    async def _is_logged_in(self) -> bool:
        """After login, the page shows a chat input and no login button visible."""
        try:
            page = self._page
            # The chat input has this placeholder
            chat_input = page.locator(
                'textarea[placeholder*="发消息"], textarea[placeholder*="输入"]'
            )
            login_btn = page.locator("button:has-text('登录'):visible")
            has_input = await chat_input.count() > 0

            # If no login button is visible and there's a chat input → logged in
            # But the home page also shows textarea before login, so check for
            # user avatar or absence of login button
            no_login_btn = await login_btn.count() == 0
            return has_input and no_login_btn
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Core: collect a single query
    # ------------------------------------------------------------------
    async def collect_one(
        self, query_id: str, query_text: str, *, _skip_claim: bool = False,
        _query_updated_at=None,
    ) -> dict:
        """Send query, wait for response, extract deep-thinking links.

        Returns: {answer_text, deep_thinking_links, all_links, link_ids}
        Args:
            _skip_claim: True when retrying from risk recovery (status already 'processing').
            _query_updated_at: updated_at from claim (for optimistic lock). Pass from batch_collect.
        """
        logger.info("Web-collecting %s: %s", query_id, query_text[:60])
        query_updated_at = _query_updated_at
        if not _skip_claim:
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
                return {"answer_text": "", "deep_thinking_links": [], "all_links": [], "link_ids": [], "skipped": True}
            query_updated_at = row["updated_at"]
        else:
            row = execute_returning(
                "UPDATE qa_query SET status = 'processing' WHERE query_id = %s "
                + sb.returning_clause(["updated_at"]),
                (query_id,),
                returning_select="SELECT updated_at FROM qa_query WHERE query_id = %s",
                returning_params=(query_id,),
            )
            if row:
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
            await self._navigate_new_chat()
            if not await self._is_logged_in():
                raise RuntimeError(
                    "Not logged in after navigation. Session may have expired. "
                    "Please run: python integration/run.py web-login --manual"
                )
            await self._ensure_default_chat_ready()
            await self._switch_to_think_mode()
            await self._send_message(query_text)
            await self._wait_until_done()

            answer_text = await self._get_answer_text()
            deep_links = await self._click_and_extract_deep_thinking()
            inline_links = await self._get_answer_inline_links()

            all_links = _merge_links(deep_links, inline_links)
            if not (answer_text or "").strip():
                raise RuntimeError(
                    "Empty answer captured from Doubao web; likely login/session/captcha/network issue."
                )
            # 思考模式下通常会有参考链接，但也存在真实无引用回答。
            # 对这类回答按成功处理，避免误判为采集失败。
            if len(all_links) == 0:
                logger.warning(
                    "No citation links captured for %s; treat as success with zero citations",
                    query_id,
                )
            link_ids = _persist_links(query_id, all_links)
            # has_citation / citation_count 以实际写入 qa_link 的数量为准
            _persist_answer(query_id, answer_text, all_links, citation_count=len(link_ids))

            n = execute(
                "UPDATE qa_query SET status = 'done' WHERE query_id = %s"
                + (" AND updated_at = %s" if query_updated_at else ""),
                (query_id, query_updated_at) if query_updated_at else (query_id,),
            )
            if query_updated_at and n == 0:
                logger.warning("Query %s optimistic lock failed (done), row was modified by another process", query_id)
            logger.info(
                "Done %s — %d chars, %d deep links, %d total links",
                query_id, len(answer_text), len(deep_links), len(all_links),
            )
            return {
                "answer_text": answer_text,
                "deep_thinking_links": deep_links,
                "all_links": all_links,
                "link_ids": link_ids,
            }
        except Exception as exc:
            logger.exception("Failed %s: %s", query_id, exc)
            q_params = (str(exc)[:500], query_id, query_updated_at) if query_updated_at else (str(exc)[:500], query_id)
            n = execute(
                "UPDATE qa_query SET status = 'error', error_message = %s, "
                "retry_count = COALESCE(retry_count, 0) + 1 WHERE query_id = %s"
                + (" AND updated_at = %s" if query_updated_at else ""),
                q_params,
            )
            if query_updated_at and n == 0:
                logger.warning("Query %s optimistic lock failed (error), row was modified by another process", query_id)
            execute(
                "UPDATE qa_answer SET status = 'error' WHERE query_id = %s",
                (query_id,),
            )
            raise

    async def batch_collect(self, batch_size: int = 5) -> list[str]:
        from shared.claim_functions import claim_pending_queries
        rows = claim_pending_queries(batch_size)
        if not rows:
            logger.info("No pending queries")
            return []

        done: list[str] = []
        for i, row in enumerate(rows):
            try:
                await self.collect_one(
                    row["query_id"], row["query_text"],
                    _query_updated_at=row.get("updated_at"),
                )
                done.append(row["query_id"])
            except Exception:
                logger.exception("Failed %s", row["query_id"])
            if i < len(rows) - 1:
                logger.info("Waiting %ds before next...", QUERY_INTERVAL)
                await asyncio.sleep(QUERY_INTERVAL)

        logger.info("Batch: %d/%d succeeded", len(done), len(rows))
        return done

    # ------------------------------------------------------------------
    # Debug / test helpers
    # ------------------------------------------------------------------
    async def dump_page(self, tag: str = "debug"):
        """Save page HTML + screenshot for inspection."""
        out_html = STATE_DIR / f"{tag}.html"
        out_png = STATE_DIR / f"{tag}.png"
        html = await self._page.content()
        out_html.write_text(html, encoding="utf-8")
        await self._page.screenshot(path=str(out_png), full_page=True)
        print(f"HTML  → {out_html}")
        print(f"截图  → {out_png}")

    async def test_one_query(self, query_text: str = "低糖水果坚果麦片推荐") -> dict:
        """End-to-end test: send a query and show what we can extract.
        Does NOT write to DB — purely for debugging selectors.
        """
        print(f"\n=== Test query: {query_text} ===\n")

        await self._navigate_new_chat()
        await self._send_message(query_text)
        print("Query sent, waiting for response...")
        await self._wait_until_done()

        # Dump DOM after response for selector debugging
        await self.dump_page("after_response")

        answer = await self._get_answer_text()
        print(f"\n--- Answer ({len(answer)} chars) ---")
        print(answer[:500] + "..." if len(answer) > 500 else answer)

        deep_links = await self._click_and_extract_deep_thinking()
        inline_links = await self._get_answer_inline_links()

        # Dump again after expanding deep-thinking
        await self.dump_page("after_deep_thinking")

        all_links = _merge_links(deep_links, inline_links)

        print(f"\n--- Deep-thinking links ({len(deep_links)}) ---")
        for lnk in deep_links:
            print(f"  [{lnk.get('source', '?')}] {lnk['url'][:100]}")
            if lnk.get("title"):
                print(f"    title: {lnk['title'][:80]}")

        print(f"\n--- Inline links ({len(inline_links)}) ---")
        for lnk in inline_links:
            print(f"  {lnk['url'][:100]}")

        print(f"\n--- Total merged ({len(all_links)}) ---")
        for lnk in all_links:
            print(f"  [{lnk['platform']}] [{lnk['content_format']}] {lnk['url'][:80]}")

        return {
            "answer_text": answer,
            "deep_thinking_links": deep_links,
            "inline_links": inline_links,
            "all_links": all_links,
        }

    # ------------------------------------------------------------------
    # Browser interaction
    # ------------------------------------------------------------------
    async def _navigate_new_chat(self):
        page = self._page
        await page.goto(DOUBAO_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)
        await self._try_open_new_chat()

    async def _try_open_new_chat(self):
        """Best-effort click for default/new chat entry."""
        page = self._page
        selectors = [
            "button:has-text('新对话')",
            "button:has-text('新建对话')",
            "button:has-text('新建聊天')",
            "[role='button']:has-text('新对话')",
        ]
        for sel in selectors:
            loc = page.locator(sel)
            if await loc.count() == 0:
                continue
            try:
                if await loc.first.is_visible():
                    await loc.first.click()
                    await page.wait_for_timeout(500)
                    logger.info("Opened default/new chat via selector: %s", sel)
                    return
            except Exception:
                continue

    async def _chat_ready_snapshot(self) -> dict:
        page = self._page
        textarea = page.locator("textarea:visible")
        textarea_count = await textarea.count()
        target_enabled = False
        target_editable = False
        if textarea_count > 0:
            try:
                target_enabled = await textarea.first.is_enabled()
                target_editable = await textarea.first.is_editable()
            except Exception:
                target_enabled = False
                target_editable = False
        modal_visible = await page.locator(
            "[role='dialog']:visible, .semi-modal:visible, [class*='modal']:visible"
        ).count() > 0
        login_required = await self._check_login_required()
        captcha_detected = await self._check_captcha()
        return {
            "textarea_count": textarea_count,
            "target_enabled": target_enabled,
            "target_editable": target_editable,
            "modal_visible": modal_visible,
            "login_required": login_required,
            "captcha_detected": captcha_detected,
        }

    async def _ensure_default_chat_ready(self):
        snap = await self._chat_ready_snapshot()
        logger.info(
            "[chat_ready] textarea_count=%d enabled=%s editable=%s modal=%s login_required=%s captcha=%s",
            snap["textarea_count"],
            snap["target_enabled"],
            snap["target_editable"],
            snap["modal_visible"],
            snap["login_required"],
            snap["captcha_detected"],
        )
        if snap["textarea_count"] > 0 and snap["target_enabled"] and snap["target_editable"]:
            return
        await self._try_open_new_chat()
        snap2 = await self._chat_ready_snapshot()
        logger.warning(
            "[chat_ready_recheck] textarea_count=%d enabled=%s editable=%s modal=%s login_required=%s captcha=%s",
            snap2["textarea_count"],
            snap2["target_enabled"],
            snap2["target_editable"],
            snap2["modal_visible"],
            snap2["login_required"],
            snap2["captcha_detected"],
        )
        if snap2["textarea_count"] > 0 and snap2["target_enabled"] and snap2["target_editable"]:
            return
        raise ChatNotReadyError(
            "chat_not_ready: default chat input is not available/editable; "
            "possible causes: not selected default conversation, login modal, or captcha overlay."
        )

    async def _switch_to_think_mode(self):
        """Switch to '思考' mode (enables web search + deep thinking)."""
        page = self._page
        mode_btn = page.locator('button:has-text("快速")')
        if await mode_btn.count() > 0:
            await mode_btn.first.click()
            await page.wait_for_timeout(1000)
            think_opt = page.locator('text=思考')
            for i in range(await think_opt.count()):
                if await think_opt.nth(i).is_visible():
                    await think_opt.nth(i).click()
                    logger.info("Switched to 思考 mode")
                    await page.wait_for_timeout(1000)
                    return True
        # Already in 思考 mode or button text differs
        think_btn = page.locator('button:has-text("思考")')
        if await think_btn.count() > 0:
            logger.info("Already in 思考 mode")
            return True
        logger.warning("Could not switch to 思考 mode, using default")
        return False

    async def _send_message(self, text: str):
        page = self._page
        ta_loc = page.locator("textarea:visible")
        ta_count = await ta_loc.count()
        if ta_count == 0:
            raise ChatNotReadyError("chat_not_ready: no visible textarea found before send")
        ta = ta_loc.first
        logger.info("[pre_send] textarea_count=%d", ta_count)
        await ta.click()
        await page.wait_for_timeout(500)
        await ta.fill(text)
        await page.wait_for_timeout(1000)
        fill_len = len(await ta.input_value())
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(2000)
        post_text = (await ta.input_value()).strip()
        generating = await page.locator(
            'button:has-text("停止"), button:has-text("停止生成"), [class*="stop"]'
        ).count() > 0
        logger.info(
            "[post_send] fill_len=%d remained_input_len=%d generating=%s",
            fill_len,
            len(post_text),
            generating,
        )
        if fill_len == 0:
            raise RuntimeError("send_failed: textarea fill is empty before Enter")
        if len(post_text) > 0 and not generating:
            logger.warning("send_maybe_failed: input text still present and no generating indicator")
        await self._check_blocking_gate_after_send()

    async def _check_blocking_gate_after_send(self, wait_ms: int = 8000):
        """Immediately after send, detect login/captcha gate instead of passive waiting."""
        page = self._page
        end_ts = time.time() + max(1, int(wait_ms / 1000))
        while time.time() < end_ts:
            if await self._check_login_required():
                raise RuntimeError(
                    "Post-send gate: session expired or login required right after submit."
                )
            if await self._check_captcha():
                raise HumanVerificationRequired(
                    "Post-send gate: CAPTCHA/human verification detected right after submit."
                )
            generating = await page.locator(
                'button:has-text("停止"), button:has-text("停止生成"), [class*="stop"]'
            ).count() > 0
            if generating:
                logger.info("Post-send check: generation indicator detected")
                return
            if (await self._get_answer_text()).strip():
                logger.info("Post-send check: answer text already appeared")
                return
            await page.wait_for_timeout(1000)

    async def _check_captcha(self) -> bool:
        """Return True if a CAPTCHA / human verification is blocking the page."""
        try:
            if await self._page.locator("#captcha_container").count() > 0:
                return True
            for frame in self._page.frames:
                if frame == self._page.main_frame:
                    continue
                if "rmc.bytedance.com" in frame.url and "captcha" in frame.url:
                    return True
        except Exception:
            pass

        try:
            body = await self._page.locator("body").inner_text()
        except Exception:
            return False
        signals = [
            "请选择所有符合",
            "拖拽到下方",
            "请完成安全验证",
            "请完成人机验证",
            "行为异常",
            "访问受限",
            "请完成验证",
            "验证失败",
        ]
        if any(s in body for s in signals):
            return True
        lower_body = body.lower()
        return "captcha" in lower_body or "human verification" in lower_body

    async def _check_login_required(self) -> bool:
        """Return True if page shows login prompt (session expired / not logged in)."""
        try:
            body = await self._page.locator("body").inner_text()
            # 登录弹窗/提示
            if "请先登录" in body or "登录后使用" in body or "请登录" in body:
                login_btn = self._page.locator("button:has-text('登录'):visible")
                if await login_btn.count() > 0:
                    return True
        except Exception:
            pass
        return False

    async def _wait_until_done(self, timeout_s: int = 300):
        """Wait until the AI finishes streaming its response.

        Handles 思考 mode's longer processing (搜索 + 思考).
        验证码：先尝试自动求解（滑块/图片选择），失败则人力兜底。
        """
        start = time.time()
        prev = ""
        stable = 0
        captcha_warned = False
        while time.time() - start < timeout_s:
            # 发送后各阶段都要频繁检查登录和验证码，避免“只等待不检测”
            if await self._check_login_required():
                raise RuntimeError(
                    "Session expired or login required. Please re-login (web-login --manual)."
                )
            if await self._check_captcha():
                if not captcha_warned:
                    logger.warning("CAPTCHA detected, attempting auto-solve...")
                    captcha_warned = True

                # 1. 尝试自动求解（integration/captcha_solver.py）
                try:
                    from integration.captcha_solver import try_solve_captcha

                    solved, err = await try_solve_captcha(self._page)
                    if solved:
                        logger.info("CAPTCHA auto-solved, continuing")
                        captcha_warned = False
                        await self._page.wait_for_timeout(2000)
                        continue
                    if err:
                        logger.debug("Auto-solve: %s", err)
                except Exception as e:
                    logger.debug("Captcha auto-solve error: %s", e)

                # 2. 自动求解失败 → 人力兜底
                print(
                    "[ACTION_REQUIRED] 验证码自动求解未成功。"
                    + (
                        "当前为无头模式，请执行 `python integration/run.py web-login --manual` 完成验证后重试。"
                        if self.headless
                        else "请在当前浏览器窗口手动完成验证，完成后将自动继续。"
                    )
                )
                if self.headless:
                    raise HumanVerificationRequired(
                        "CAPTCHA/human verification detected in headless mode."
                    )
                await self._page.wait_for_timeout(3000)
                continue

            if captcha_warned:
                logger.info("CAPTCHA resolved")
                captcha_warned = False

            cur = await self._get_answer_text()
            if cur and cur == prev:
                stable += 1
                if stable >= 5:
                    break
            else:
                stable = 0
            prev = cur
            await self._page.wait_for_timeout(2000)

        final_answer = await self._get_answer_text()
        if not final_answer.strip():
            # 超时前 dump 页面便于排查（登录/人机验证/选择器问题）
            try:
                await self.dump_page("timeout_before_fail")
                logger.warning(
                    "Dumped page to integration/.browser_state/ for timeout debugging"
                )
            except Exception as e:
                logger.debug("Dump on timeout failed: %s", e)
            diag = await self._chat_ready_snapshot()
            logger.error(
                "[timeout_diag] login_required=%s captcha_detected=%s textarea_count=%d enabled=%s editable=%s modal=%s user_msg_seen=%s generating_seen=%s",
                diag["login_required"],
                diag["captcha_detected"],
                diag["textarea_count"],
                diag["target_enabled"],
                diag["target_editable"],
                diag["modal_visible"],
                False,
                await self._page.locator(
                    'button:has-text("停止"), button:has-text("停止生成"), [class*="stop"]'
                ).count() > 0,
            )
            raise RuntimeError(
                "Timed out waiting for Doubao answer content. "
                "Possible causes: login expired, human verification, or network. "
                "Check integration/.browser_state/timeout_before_fail.html for page state."
            )
        await self._page.wait_for_timeout(3000)

    async def _get_answer_text(self) -> str:
        """Get the visible text of the last AI response message."""
        try:
            for sel in [".markdown-body", '[class*="flow-markdown"]',
                        '[class*="message-content"]']:
                loc = self._page.locator(sel)
                if await loc.count() > 0:
                    txt = (await loc.last.inner_text()).strip()
                    if txt:
                        return txt
        except Exception:
            pass
        return ""

    # ------------------------------------------------------------------
    # Deep-thinking link extraction (the key feature)
    # ------------------------------------------------------------------
    async def _extract_deep_thinking_links_js(self, page) -> list[dict]:
        """从参考资料侧栏/思考块等 DOM 中抽取外链；选择器兼容豆包多版本与 class 哈希。"""
        links: list[dict] = await page.evaluate("""() => {
            const results = [];
            const seen = new Set();
            const skipDomains = ['doubao.com', 'volces.com', 'bytedance.com', 'byteimg.com', 'feishu.cn', 'javascript:'];
            const isOk = (url) => url && url.startsWith('http') && !skipDomains.some(d => url.includes(d));

            function collectFromRoot(root) {
                if (!root || !root.querySelectorAll) return;
                root.querySelectorAll('a[href^="http"]').forEach(a => {
                    const url = (a.href || '').trim();
                    if (!isOk(url) || seen.has(url)) return;
                    seen.add(url);
                    results.push({
                        url: url,
                        title: (a.textContent || '').trim().slice(0, 300),
                        source: 'deep_thinking',
                    });
                });
            }

            // 1) 从可能的参考资料/思考/侧栏容器中取链接（豆包 class 可能含 think/search/reference/drawer/panel 等）
            const containerSelectors = [
                '[class*="think"]', '[class*="deep"]', '[class*="search"]',
                '[class*="reference"]', '[class*="ref"]', '[class*="source"]',
                '[class*="citation"]', '[class*="collapse"]', '[class*="flow"]',
                '[class*="reasoning"]', '[class*="process"]',
                '[class*="drawer"]', '[class*="sidebar"]', '[class*="panel"]',
                '[class*="modal"]', '[class*="popover"]', '[class*="overlay"]',
                '[class*="link"]', '[class*="url"]', '[class*="content"]',
                'details[open]', '[role="dialog"]', '[aria-label*="参考"]', '[aria-label*="资料"]',
            ];
            for (const sel of containerSelectors) {
                try {
                    document.querySelectorAll(sel).forEach(collectFromRoot);
                } catch (e) {}
            }

                // 2) 从包含「参考」「资料」等文案的节点子树中取链接（结构未知时兜底）
            const walk = (node) => {
                if (!node) return;
                const text = (node.innerText || '').slice(0, 200);
                if ((/参考|资料|来源|引用|搜索.*关键词/).test(text))
                    collectFromRoot(node);
                for (const child of node.children || []) walk(child);
            };
            walk(document.body);

            // 3) 若有 iframe，尝试从其 document 抽取
            document.querySelectorAll('iframe').forEach(iframe => {
                try {
                    const doc = iframe.contentDocument || iframe.contentWindow?.document;
                    if (doc && doc.body) collectFromRoot(doc.body);
                } catch (e) {}
            });

            return results;
        }""")
        return links

    async def _click_and_extract_deep_thinking(self) -> list[dict]:
        """Expand 深度思考 section and collect links step-by-step with dedupe."""
        page = self._page
        merged_links: list[dict] = []

        async def collect_once(stage: str) -> int:
            nonlocal merged_links
            links = await self._extract_deep_thinking_links_js(page)
            if not links:
                links = await self._extract_urls_from_thinking_text()
            if links:
                before = len(merged_links)
                merged_links = _merge_unique_links(merged_links, links)
                delta = len(merged_links) - before
                logger.info("Deep-thinking collect@%s: +%d (total=%d)", stage, delta, len(merged_links))
                return delta
            return 0

        # 1) Expand main thinking block ("已完成思考，参考 N 篇资料")
        collapse_btn = page.locator("[class*='collapse-collapse-button'], [class*='think-block-title']")
        for i in range(await collapse_btn.count()):
            try:
                txt = (await collapse_btn.nth(i).inner_text()).strip()
                if "完成思考" in txt or "参考" in txt:
                    await collapse_btn.nth(i).click()
                    logger.info("Expanded thinking block: %s", txt[:50])
                    await page.wait_for_timeout(1500)
                    await collect_once("expand_thinking")
                    break
            except Exception:
                continue

        # 2) Click each "搜索 x 个关键词" step and collect immediately.
        # Run multi-round scan because new step buttons can appear after previous clicks.
        click_count = 0
        for _round in range(8):
            progressed = False
            search_like_btns = page.locator("button, [role='button'], [class*='searchBtn'], [class*='entry-btn']")
            for i in range(await search_like_btns.count()):
                try:
                    el = search_like_btns.nth(i)
                    if not await el.is_visible():
                        continue
                    txt = (await el.inner_text()).strip()
                    if "搜索" not in txt or "关键词" not in txt:
                        continue
                    # Mark this DOM node so we won't click the same step twice.
                    is_new = await el.evaluate(
                        """(node) => {
                            if (node.dataset.cursorStepClicked === '1') return false;
                            node.dataset.cursorStepClicked = '1';
                            return true;
                        }"""
                    )
                    if not is_new:
                        continue
                    await el.click()
                    click_count += 1
                    progressed = True
                    logger.info("Opened deep-thinking step %d: %s", click_count, txt[:50])
                    await page.wait_for_timeout(1500)
                    await collect_once(f"step_{click_count}")
                except Exception:
                    continue
            if not progressed:
                break

        # 3) Always do one full-page pass to reduce omission risk for dynamic DOM.
        page_links = await self._extract_all_page_links()
        if page_links:
            before = len(merged_links)
            merged_links = _merge_unique_links(merged_links, page_links)
            delta = len(merged_links) - before
            if delta > 0:
                logger.info("Final page scan added %d links", delta)

        # 4) Fallback dump only if still empty.
        if not merged_links:
            try:
                await self.dump_page("after_reference_panel")
                logger.debug("Dumped DOM to .browser_state/after_reference_panel.html for selector debugging")
            except Exception:
                pass

        logger.info("Extracted %d deep-thinking links", len(merged_links))
        return merged_links

    async def _extract_urls_from_thinking_text(self) -> list[dict]:
        """Read text from thinking containers and regex-extract URLs."""
        texts: list[str] = await self._page.evaluate("""() => {
            const t = [];
            const sels = [
                '[class*="think"]', '[class*="deep"]', '[class*="search"]',
                '[class*="reasoning"]', '[class*="process"]',
                '[class*="flow"]', 'details[open]',
            ];
            for (const s of sels) {
                document.querySelectorAll(s).forEach(el => {
                    t.push(el.innerText || '');
                });
            }
            return t;
        }""")
        seen: set[str] = set()
        results: list[dict] = []
        for text in texts:
            for url in _URL_RE.findall(text):
                url = url.rstrip(".,;:!?)")
                if url not in seen and "doubao.com" not in url and "volces.com" not in url:
                    seen.add(url)
                    results.append({"url": url, "title": "", "source": "deep_thinking_text"})
        if results:
            logger.info("Fallback text extraction found %d URLs", len(results))
        return results

    async def _extract_all_page_links(self) -> list[dict]:
        """Last resort: grab all external <a> links on the page."""
        links: list[dict] = await self._page.evaluate("""() => {
            const results = [];
            const seen = new Set();
            const skip = ['doubao.com', 'volces.com', 'bytedance.com',
                          'feishu.cn', 'byteimg.com'];
            document.querySelectorAll('a[href^="http"]').forEach(a => {
                const url = a.href;
                if (!seen.has(url) && !skip.some(d => url.includes(d))) {
                    seen.add(url);
                    results.push({
                        url: url,
                        title: (a.textContent || '').trim().slice(0, 300),
                        source: 'page_fallback',
                    });
                }
            });
            return results;
        }""")
        if links:
            logger.info("Page-level fallback found %d links", len(links))
        return links

    async def _get_answer_inline_links(self) -> list[dict]:
        """Links embedded in the answer text itself (citation links)."""
        links: list[dict] = await self._page.evaluate("""() => {
            const results = [];
            const seen = new Set();
            document.querySelectorAll(
                '.markdown-body a[href], '
                + '[class*="answer"] a[href], '
                + '[class*="message-content"] a[href]'
            ).forEach(a => {
                const url = a.href;
                if (url && url.startsWith('http') && !seen.has(url)
                    && !url.includes('doubao.com')) {
                    seen.add(url);
                    results.push({
                        url: url,
                        title: (a.textContent || '').trim().slice(0, 300),
                        source: 'answer_inline',
                    });
                }
            });
            return results;
        }""")
        return links


# ======================================================================
# Module-level helpers
# ======================================================================
def _merge_links(deep: list[dict], inline: list[dict]) -> list[dict]:
    seen: set[str] = set()
    merged: list[dict] = []
    for lnk in deep + inline:
        url = lnk["url"].rstrip("/")
        if url in seen:
            continue
        seen.add(url)
        platform = identify_platform(url)
        merged.append({
            **lnk,
            "platform": platform,
            "content_format": determine_content_format(url, platform),
        })
    return merged


def _merge_unique_links(existing: list[dict], incoming: list[dict]) -> list[dict]:
    """Merge links by canonical URL (strip trailing slash), keep first-seen payload."""
    seen: set[str] = set()
    merged: list[dict] = []
    for lnk in existing + incoming:
        raw_url = (lnk.get("url") or "").strip()
        if not raw_url:
            continue
        url = raw_url.rstrip("/")
        if not url or url in seen:
            continue
        seen.add(url)
        merged.append(lnk)
    return merged


def _persist_answer(query_id: str, text: str, links: list[dict], citation_count: int | None = None):
    """写入 qa_answer；has_citation/citation_count 优先使用 citation_count（与 qa_link 实际条数一致）。"""
    if citation_count is None:
        citation_count = len(links)
    has_citation = citation_count > 0
    raw = json.dumps({"source": "web_ui", "links": links}, ensure_ascii=False)
    existing = fetch_one("SELECT id FROM qa_answer WHERE query_id = %s", (query_id,))
    if existing:
        execute(
            "UPDATE qa_answer SET answer_text=%s, answer_length=%s, "
            "status=%s, has_citation=%s, citation_count=%s, raw_data=%s WHERE query_id=%s",
            (text, len(text), "done", has_citation, citation_count, raw, query_id),
        )
    else:
        execute(
            "INSERT INTO qa_answer "
            "(query_id, answer_text, answer_length, status, has_citation, citation_count, raw_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (query_id, text, len(text), "done", has_citation, citation_count, raw),
        )


def _persist_links(query_id: str, links: list[dict]) -> list[str]:
    """将参考资料/深度思考链接写入 qa_link 表（参考 N 篇资料必须落库）。"""
    link_ids: list[str] = []
    valid_links = []
    for lnk in links:
        url = (lnk.get("url") or "").strip()
        if not url or not url.startswith("http"):
            continue
        valid_links.append(lnk)
    for i, lnk in enumerate(valid_links, 1):
        link_id = f"{query_id}_L{i:03d}"
        url = (lnk.get("url") or "").strip()
        platform = (lnk.get("platform") or "其他")[:64]
        content_format = (lnk.get("content_format") or "图文B")[:32]
        old = fetch_one(
            "SELECT link_url, platform, content_format FROM qa_link WHERE link_id = %s",
            (link_id,),
        )
        _upsert_suffix = sb.upsert_suffix(
            ["link_id"],
            ["query_id", "link_url", "platform", "content_format"],
        )
        n = execute(
            "INSERT INTO qa_link "
            "(query_id, link_id, link_url, platform, content_format, status) "
            "VALUES (%s, %s, %s, %s, %s, 'pending') "
            + _upsert_suffix
            + ", publish_time=NULL, popularity=NULL, fetched_at=NULL, "
            "status='pending', updated_at=CURRENT_TIMESTAMP",
            (query_id, link_id, url, platform, content_format),
        )
        # If link target/type changed for same link_id, old content_json becomes stale.
        # Delete it so crawl will rebuild the correct shape.
        if old and (
            (old.get("link_url") or "").strip() != url
            or (old.get("platform") or "").strip() != platform
            or (old.get("content_format") or "").strip() != content_format
        ):
            execute("DELETE FROM qa_link_content WHERE link_id = %s", (link_id,))
            logger.info("Cleared stale qa_link_content for %s due to link metadata change", link_id)
        link_ids.append(link_id)
        if n and n > 0:
            logger.debug("Wrote qa_link %s", link_id)
    if link_ids:
        logger.info("Persisted %d reference links to qa_link for %s", len(link_ids), query_id)
    # Remove stale tail links when current extraction has fewer links than
    # previous runs of the same query (e.g., old Q0001_L021~L030).
    not_frag, not_params = sb.expand_not_all("link_id", link_ids or [""])
    stale_rows = fetch_all(
        f"SELECT link_id FROM qa_link WHERE query_id = %s AND {not_frag}",
        (query_id, *not_params),
    )
    for row in stale_rows:
        stale_id = row["link_id"]
        execute("DELETE FROM qa_link_content WHERE link_id = %s", (stale_id,))
        execute("DELETE FROM qa_link WHERE link_id = %s", (stale_id,))
    if stale_rows:
        logger.info("Removed %d stale links for %s", len(stale_rows), query_id)
    return link_ids


# ======================================================================
# CLI
# ======================================================================
async def _cli():
    import argparse

    p = argparse.ArgumentParser(description="Doubao web collector (deep-thinking)")
    p.add_argument(
        "action",
        choices=["login", "collect", "batch", "test", "debug"],
    )
    p.add_argument("--query-id", help="Specific query_id")
    p.add_argument("--query-text", help="Custom query text for test mode")
    p.add_argument("--batch-size", type=int, default=5)
    p.add_argument("--headed", action="store_true", help="Run with visible browser window")
    p.add_argument("--manual", action="store_true", help="Use manual login instead of SMS")
    args = p.parse_args()

    c = DoubaoWebCollector(headless=not args.headed)
    await c.start()

    try:
        if args.action == "login":
            if args.manual:
                await c.manual_login()
            else:
                ok = await c.auto_login()
                if not ok:
                    print("Auto-login failed. Try --manual instead.")

        elif args.action == "debug":
            await c._page.goto(DOUBAO_URL, wait_until="domcontentloaded")
            await c._page.wait_for_timeout(3000)
            await c.dump_page("debug")

        elif args.action == "test":
            if not await c._is_logged_in():
                await c._page.goto(DOUBAO_URL, wait_until="domcontentloaded")
                await c._page.wait_for_timeout(3000)
                if not await c._is_logged_in():
                    print("Not logged in. Run `login` first.")
                    return
            qt = args.query_text or "低糖水果坚果麦片推荐"
            await c.test_one_query(qt)

        elif args.action == "collect":
            if args.query_id:
                row = fetch_one(
                    "SELECT query_id, query_text FROM qa_query WHERE query_id = %s",
                    (args.query_id,),
                )
            else:
                row = fetch_one(
                    "SELECT query_id, query_text FROM qa_query "
                    "WHERE status = 'pending' ORDER BY id LIMIT 1"
                )
            if not row:
                print("No query found.")
                return
            result = await c.collect_one(row["query_id"], row["query_text"])
            print(f"\nAnswer ({len(result['answer_text'])} chars)")
            print(f"Deep-thinking links: {len(result['deep_thinking_links'])}")
            print(f"Total links: {len(result['all_links'])}")
            for lnk in result["all_links"]:
                print(f"  [{lnk['platform']}] {lnk['url'][:80]}")

        elif args.action == "batch":
            await c.batch_collect(args.batch_size)
    finally:
        await c.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(_cli())
