"""豆包图形验证码自动求解：滑块 + 图片选择，人力兜底。

参考 docs/图形验证码-识别与拖拽逻辑.md 实现。
- 滑块：Python OpenCV 识别缺口 + easeOutCubic 缓动拖拽
- 图片选择：火山云 Vision LLM 识别 + 贝塞尔曲线拖拽
"""

from __future__ import annotations

import base64
import logging
import random
from typing import Any

logger = logging.getLogger(__name__)

# OpenCV 可选：未安装时滑块求解不可用
try:
    import cv2
    import numpy as np
    HAS_OPENCV = True
except ImportError:
    HAS_OPENCV = False


# =============================================================================
# 滑块验证码：OpenCV 缺口识别
# =============================================================================


def _solve_slide_opencv(bg_b64: str, target_b64: str) -> tuple[int, int]:
    """识别滑块缺口位置，返回 (x, y) 缺口左上角坐标。

    算法：灰度 → 高斯模糊 → Canny 边缘 → 轮廓 → 过滤与匹配。
    """
    if not HAS_OPENCV:
        raise RuntimeError("opencv-python-headless not installed; pip install opencv-python-headless")

    def decode_b64(b64: str, flags: int):
        raw = base64.b64decode(b64)
        arr = np.frombuffer(raw, dtype=np.uint8)
        return cv2.imdecode(arr, flags)

    bg = decode_b64(bg_b64, cv2.IMREAD_GRAYSCALE)
    target = decode_b64(target_b64, cv2.IMREAD_UNCHANGED)
    if bg is None or target is None:
        raise ValueError("Failed to decode captcha images")

    tpl_h, tpl_w = target.shape[:2]
    blurred = cv2.GaussianBlur(bg, (3, 3), 0)
    edges = cv2.Canny(blurred, 100, 200)
    contours, _ = cv2.findContours(
        edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )

    min_w = int(tpl_w * 0.8)
    min_h = int(tpl_h * 0.8)
    exclude_left = bg.shape[1] // 5
    best_rect = None
    min_diff = float("inf")

    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        if w < min_w or h < min_h:
            continue
        if x < exclude_left:
            continue
        diff = abs(w - tpl_w) + abs(h - tpl_h)
        if diff < min_diff:
            min_diff = diff
            best_rect = (x, y, w, h)

    if best_rect:
        return best_rect[0], best_rect[1]
    return bg.shape[1] // 2, bg.shape[0] // 2


# =============================================================================
# 人类化拖拽：缓动曲线
# =============================================================================


def _ease_out_cubic(t: float) -> float:
    return 1 - (1 - t) ** 3


def _generate_bezier_path(
    x0: float, y0: float, x3: float, y3: float, steps: int = 50
) -> list[tuple[float, float]]:
    """三次贝塞尔曲线路径，控制点带随机扰动。"""
    dx = x3 - x0
    dy = y3 - y0
    x1 = x0 + dx * (0.2 + random.random() * 0.3) + (random.random() - 0.5) * 100
    y1 = y0 + dy * (0.2 + random.random() * 0.3) + (random.random() - 0.5) * 100
    x2 = x0 + dx * (0.5 + random.random() * 0.3) + (random.random() - 0.5) * 100
    y2 = y0 + dy * (0.5 + random.random() * 0.3) + (random.random() - 0.5) * 100

    points = []
    for i in range(steps + 1):
        t = i / steps
        t1 = 1 - t
        x = t1**3 * x0 + 3 * t1**2 * t * x1 + 3 * t1 * t**2 * x2 + t**3 * x3
        y = t1**3 * y0 + 3 * t1**2 * t * y1 + 3 * t1 * t**2 * y2 + t**3 * y3
        points.append((x, y))
    return points


async def _human_like_slide_drag(page, start_x: float, start_y: float, distance: float):
    """滑块缓动拖拽：easeOutCubic + Y 轴抖动 + 末尾过冲回正。"""
    direction = 1 if distance >= 0 else -1
    abs_dist = max(abs(distance), 10)
    total_time = 900 + random.random() * 400
    steps = 30 + int(random.random() * 20)

    await page.mouse.move(start_x, start_y)
    await page.mouse.down()

    prev_x, prev_y = start_x, start_y
    start_time = __import__("time").time()

    for i in range(1, steps + 1):
        t = i / steps
        eased = _ease_out_cubic(t)
        target_x = start_x + direction * abs_dist * eased
        jitter_y = (random.random() - 0.5) * 4
        next_y = start_y + jitter_y
        dx = target_x - prev_x
        dy = next_y - prev_y

        await page.mouse.move(prev_x + dx, prev_y + dy, steps=2)
        prev_x += dx
        prev_y += dy

        elapsed = (__import__("time").time() - start_time) * 1000
        remaining = total_time - elapsed
        remaining_steps = steps - i
        sleep_ms = max(5, remaining / remaining_steps) if remaining_steps > 0 else 0
        if sleep_ms > 0:
            await page.wait_for_timeout(int(sleep_ms))

    adjust = 3 + random.random() * 3
    await page.mouse.move(prev_x - direction * adjust, prev_y, steps=3)
    await page.wait_for_timeout(int(80 + random.random() * 120))
    await page.mouse.move(prev_x, prev_y, steps=2)
    await page.wait_for_timeout(int(120 + random.random() * 180))
    await page.mouse.up()


async def _human_like_bezier_drag(
    page, start_x: float, start_y: float, end_x: float, end_y: float
):
    """贝塞尔曲线拖拽（图片选择用）。"""
    path = _generate_bezier_path(start_x, start_y, end_x, end_y, 50)
    await page.mouse.move(start_x, start_y)
    await page.wait_for_timeout(int(100 + random.random() * 100))
    await page.mouse.down()
    for px, py in path:
        await page.mouse.move(px, py)
        await page.wait_for_timeout(int(2 + random.random() * 3))
    await page.mouse.up()
    await page.wait_for_timeout(int(300 + random.random() * 400))


# =============================================================================
# 图片选择验证码：火山云 Vision LLM
# =============================================================================


async def _analyze_image_with_llm(
    question: str, image_b64: str, image_index: int
) -> bool:
    """调用火山云 Vision 模型判断图片是否匹配问题。返回 True 表示匹配。"""
    from shared.config import CONFIG

    vc = CONFIG["volcengine"]
    api_key = vc.get("api_key")
    base_url = vc.get("base_url", "https://ark.cn-beijing.volces.com/api/v3")
    model = vc.get("vision_model") or vc.get("seed_model")
    if not api_key or not model:
        raise RuntimeError("VOLCENGINE_API_KEY and VOLCENGINE_VISION_MODEL required for semantic captcha")

    import httpx

    url = f"{base_url.rstrip('/')}/chat/completions"
    body = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "你是一个图像分类专家。用户会给你一个类别描述和一张图片。"
                    "你需要判断这张图片是否属于该类别。"
                    "只回答 YES 或 NO，不要解释。"
                ),
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f'类别描述：「{question}」\n\n请判断这张图片（第 {image_index + 1} 张）是否属于上述类别？只回答 YES 或 NO。',
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/png;base64,{image_b64}"},
                    },
                ],
            },
        ],
        "temperature": 0.1,
        "max_tokens": 10,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            url,
            json=body,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        answer = (data.get("choices") or [{}])[0].get("message", {}).get("content", "") or ""
        is_match = "YES" in answer.upper()
        logger.info(
            "LLM image[%d] question='%s' → answer='%s' match=%s",
            image_index, question[:30], answer.strip(), is_match,
        )
        return is_match


# =============================================================================
# 策略检测与求解入口
# =============================================================================


def _get_actual_page(frame) -> Any:
    """验证码在 iframe 内，鼠标操作需用父 page。"""
    return frame.page if hasattr(frame, "page") else frame


async def _can_handle_slide(frame) -> bool:
    """检测是否为滑块验证码。"""
    slide_sel = await frame.locator(".vc-captcha-verify.slide").count()
    if slide_sel > 0:
        return True
    for sel in [".captcha-slider-btn", ".captcha-verify-image-slide", "img.captcha-verify-image"]:
        if await frame.locator(sel).count() > 0:
            return True
    body = await frame.locator("body").inner_text()
    return bool(
        body
        and (
            "拖动" in body
            or "按住" in body
            or "滑动" in body
            or "完成上方拼图" in body
        )
    )


async def _can_handle_semantic(frame) -> bool:
    """检测是否为图片选择验证码。"""
    canvas_count = await frame.locator("canvas").count()
    drag_area = await frame.locator(".drag-area").count()
    has_question = await frame.locator(".tit.captcha_verify_bar--title, .captcha-verify-title").count()
    return canvas_count > 0 and drag_area > 0 and has_question > 0


async def _solve_slide(frame) -> bool:
    """求解滑块验证码。"""
    page = _get_actual_page(frame)

    bg_sel = frame.locator("img.captcha-verify-image").first
    slide_sel = frame.locator("img.captcha-verify-image-slide").first
    if await bg_sel.count() == 0 or await slide_sel.count() == 0:
        logger.warning("Slide captcha: bg or slider image not found")
        return False

    bg_bytes = await bg_sel.screenshot(type="png")
    slide_bytes = await slide_sel.screenshot(type="png")
    if not bg_bytes or not slide_bytes:
        return False

    bg_b64 = base64.b64encode(bg_bytes).decode()
    slide_b64 = base64.b64encode(slide_bytes).decode()

    try:
        gap_x, gap_y = _solve_slide_opencv(bg_b64, slide_b64)
    except Exception as e:
        logger.warning("Slide solver failed: %s", e)
        return False

    bg_box = await bg_sel.bounding_box()
    slide_box = await slide_sel.bounding_box()
    if not bg_box or not slide_box:
        return False

    # 坐标转换：gap 在 bg 内，需加上 bg 在页面中的位置
    gap_page_x = bg_box["x"] + gap_x
    gap_page_y = bg_box["y"] + gap_y
    # 拖拽距离 = 缺口 x - 滑块相对 bg 的 x
    slider_offset_x = slide_box["x"] - bg_box["x"]
    drag_distance = gap_x - slider_offset_x

    # 起点：滑块按钮中心，若无则用滑块图中心
    btn = frame.locator(".captcha-slider-btn").first
    if await btn.count() > 0:
        btn_box = await btn.bounding_box()
        if btn_box:
            start_x = btn_box["x"] + btn_box["width"] / 2
            start_y = btn_box["y"] + btn_box["height"] / 2
        else:
            start_x = slide_box["x"] + slide_box["width"] / 2
            start_y = slide_box["y"] + slide_box["height"] / 2
    else:
        start_x = slide_box["x"] + slide_box["width"] / 2
        start_y = slide_box["y"] + slide_box["height"] / 2

    await _human_like_slide_drag(page, start_x, start_y, drag_distance)
    await page.wait_for_timeout(1500)
    return True


async def _extract_semantic_data(frame) -> dict | None:
    """从 frame 提取问题文本和 canvas 图片。"""
    data = await frame.evaluate(
        """() => {
        const qSel = ['.tit.captcha_verify_bar--title', '.captcha-verify-title', '[class*="title"]'];
        let question = '';
        for (const s of qSel) {
            const el = document.querySelector(s);
            if (el?.textContent?.trim()) { question = el.textContent.trim(); break; }
        }
        const canvases = document.querySelectorAll('canvas');
        const images = [];
        canvases.forEach((c, i) => {
            try {
                if (c.width < 20 || c.height < 20) return;
                const dataUrl = c.toDataURL('image/png');
                if (dataUrl?.startsWith('data:image') && dataUrl.length > 200) {
                    const b64 = dataUrl.split(',')[1] || '';
                    images.push({ b64, index: i, w: c.width, h: c.height, b64len: b64.length });
                }
            } catch (_) {}
        });
        return { question, images, totalCanvases: canvases.length };
    }"""
    )
    if not data or not data.get("images"):
        logger.warning("Semantic extract: no valid images found (data=%s)", data)
        return None
    logger.info(
        "Semantic extract: question='%s' images=%d totalCanvases=%d sizes=%s",
        data.get("question", "")[:40],
        len(data["images"]),
        data.get("totalCanvases", 0),
        [(img["index"], img.get("w"), img.get("h"), img.get("b64len", 0)) for img in data["images"][:5]],
    )
    return data


async def _solve_semantic(frame) -> bool:
    """求解图片选择验证码。"""
    page = _get_actual_page(frame)
    data = await _extract_semantic_data(frame)
    if not data:
        return False

    question = data.get("question", "")
    images = data.get("images", [])
    if not question or not images:
        return False

    matching_indices = []
    for img in images:
        try:
            is_match = await _analyze_image_with_llm(
                question, img["b64"], img["index"]
            )
            if is_match:
                matching_indices.append(img["index"])
        except Exception as e:
            logger.warning("LLM analyze image %s failed: %s", img["index"], e)

    logger.info(
        "Semantic captcha: question='%s' total_images=%d matching=%s",
        question[:40], len(images), matching_indices,
    )

    if not matching_indices:
        logger.warning("Semantic captcha: no matching images found")
        return False

    canvases = await frame.locator("canvas").all()
    drag_area = frame.locator(".drag-area").first
    drag_box = await drag_area.bounding_box()
    if not drag_box:
        return False

    for idx in matching_indices:
        if idx >= len(canvases):
            continue
        canvas = canvases[idx]
        box = await canvas.bounding_box()
        if not box:
            continue
        src_x = box["x"] + box["width"] / 2
        src_y = box["y"] + box["height"] / 2
        tgt_x = drag_box["x"] + drag_box["width"] / 2 + (random.random() - 0.5) * 20
        tgt_y = drag_box["y"] + drag_box["height"] / 2 + (random.random() - 0.5) * 20
        logger.info("Dragging canvas[%d] (%d,%d) -> drop zone (%d,%d)", idx, int(src_x), int(src_y), int(tgt_x), int(tgt_y))
        await _human_like_bezier_drag(page, src_x, src_y, tgt_x, tgt_y)

    # 点击提交
    submitted = False
    submit_sel = [
        ".vc-captcha-verify-pc-button",
        "button.vc-captcha-verify-pc-button",
        "button[type='submit']",
    ]
    for sel in submit_sel:
        btn = frame.locator(sel).first
        if await btn.count() > 0:
            await page.wait_for_timeout(int(200 + random.random() * 300))
            await btn.click()
            submitted = True
            logger.info("Clicked submit button: %s", sel)
            break

    if not submitted:
        logger.warning("No submit button found")
        return False

    # 验证结果：等 3 秒检查验证码是否消失
    await page.wait_for_timeout(3000)

    captcha_container = page.locator("#captcha_container")
    still_visible = await captcha_container.count() > 0
    if still_visible:
        try:
            body_text = await frame.locator("body").inner_text()
        except Exception:
            body_text = ""
        if "验证失败" in body_text or "重新操作" in body_text:
            logger.warning("Semantic captcha: VERIFICATION FAILED (server rejected answer)")
            return False
        logger.warning("Semantic captcha: captcha still visible after submit (might be loading new one)")
        return False

    logger.info("Semantic captcha: captcha container disappeared — SUCCESS")
    return True


async def solve_captcha_auto(frame, max_retries: int = 3) -> tuple[bool, str]:
    """自动求解验证码。先尝试滑块，再尝试图片选择。

    Returns:
        (success, error_message)
    """
    for attempt in range(max_retries):
        logger.info("=== solve_captcha_auto attempt %d/%d ===", attempt + 1, max_retries)

        # After a failed attempt the server may reload the captcha iframe,
        # making the old frame reference stale (TargetClosedError).
        # Re-find the frame from the parent page before each retry.
        if attempt > 0:
            try:
                await frame.locator("body").count()
            except Exception:
                logger.info("Captcha frame went stale after attempt %d, re-finding...", attempt)
                page = _get_actual_page(frame)
                new_frame = await find_captcha_frame(page)
                if not new_frame:
                    return False, "Captcha frame disappeared during retry"
                frame = new_frame
                await page.wait_for_timeout(2000)

        is_slide = await _can_handle_slide(frame)
        is_semantic = await _can_handle_semantic(frame)
        logger.info("Captcha type detection: slide=%s semantic=%s", is_slide, is_semantic)

        if is_slide:
            if not HAS_OPENCV:
                return False, "opencv-python-headless not installed for slide captcha"
            try:
                ok = await _solve_slide(frame)
                if ok:
                    logger.info("Slide captcha solved (attempt %d)", attempt + 1)
                    return True, ""
            except Exception as e:
                logger.warning("Slide solve attempt %d failed: %s", attempt + 1, e)

        if is_semantic:
            from shared.config import CONFIG

            if not CONFIG["volcengine"].get("api_key"):
                return False, "VOLCENGINE_API_KEY required for semantic captcha"
            try:
                ok = await _solve_semantic(frame)
                if ok:
                    logger.info("Semantic captcha solved (attempt %d)", attempt + 1)
                    return True, ""
                logger.warning("Semantic solve attempt %d: submitted but server rejected", attempt + 1)
            except Exception as e:
                logger.warning("Semantic solve attempt %d exception: %s", attempt + 1, e)

        if not is_slide and not is_semantic:
            logger.warning("Neither slide nor semantic captcha detected in frame")
            return False, "Unknown captcha type"

        page = _get_actual_page(frame)
        await page.wait_for_timeout(2000)

    return False, f"Auto solve failed after {max_retries} retries"


async def find_captcha_frame(page) -> Any | None:
    """在主页面中查找验证码 iframe（rmc.bytedance.com）。"""
    captcha_container = page.locator("#captcha_container")
    if await captcha_container.count() == 0:
        return None

    for frame in page.frames:
        url = frame.url
        if "rmc.bytedance.com" in url and "verifycenter/captcha" in url:
            return frame
    return None


async def try_solve_captcha(page) -> tuple[bool, str]:
    """检测并尝试自动求解验证码。

    Returns:
        (solved, message) - solved=True 表示验证码已解决，可继续流程
    """
    frame = await find_captcha_frame(page)
    if not frame:
        return False, "No captcha frame found"

    await page.wait_for_timeout(2000)
    return await solve_captcha_auto(frame)
