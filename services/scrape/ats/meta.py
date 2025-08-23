from __future__ import annotations

import json
import os
import re
import sys
from typing import List, Optional, Set, Tuple
from urllib.parse import urlparse, urljoin

from data.model import Job

# Playwright (async)
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Page,
    BrowserContext,
)

# ------- minimal debug (opt-in via env) -------
META_DEBUG = os.getenv("META_DEBUG", "").lower() in ("1", "true", "yes", "y")
META_HEADLESS = os.getenv("META_HEADLESS", "").lower() not in ("0", "false", "no", "n")  # default: headless True

def _dbg(msg: str) -> None:
    if META_DEBUG:
        print(f"[metacareers] {msg}", file=sys.stderr)

# Meta Careers uses both domains
_META_HOST_RE = re.compile(
    r"(?:^|\.)metacareers\.com$|(?:^|\.)facebookcareers\.com$",
    re.I,
)


class MetaCareersAdapter:
    pattern = _META_HOST_RE

    @staticmethod
    def matches(url: str) -> bool:
        return bool(_META_HOST_RE.search(urlparse(url).netloc))

    @staticmethod
    async def scrape(url: str, *, timeout: int = 30, max_pages: int = 5) -> List[Job]:
        """
        Scraper:
          - Warm-up: visit site root, accept cookies (longer the first time), click “View jobs”.
          - Open the target listing URL.
          - WAIT for results to render (crucial in headless).
          - Crawl pagination (“Next”, “Page X of Y”).
          - Collect job detail URLs per page.
          - Resolve titles from each job detail (div._army → heading → JSON-LD → og:title).
        """
        parsed = urlparse(url)
        base_origin = f"{parsed.scheme}://{parsed.netloc}"
        per_page_scrolls = 6
        nav_timeout_ms = timeout * 1000

        _dbg(f"scrape(url={url!r}, timeout={timeout}, max_pages={max_pages}, headless={META_HEADLESS})")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=META_HEADLESS)
            context = await browser.new_context(
                viewport={"width": 1440, "height": 1600},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            page = await context.new_page()

            # Warm-up to obtain the cookie (longer waits for cookie banner the first time)
            await _warmup_session(page, base_origin, nav_timeout_ms)

            # Go to the target listing
            try:
                _dbg(f"nav → target {url}")
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
                _dbg(f"nav status={getattr(resp, 'status', None)} url={page.url}")
            except PlaywrightTimeoutError:
                _dbg("ERROR: target navigation timeout")
                await context.close()
                await browser.close()
                return []

            # Accept cookies quickly if shown again
            await _accept_cookies(page, first_time=False)

            # 🔑 NEW: give the SPA time to actually render the results before scraping
            await _ensure_results_ready(page, nav_timeout_ms)

            # Collect URLs across pagination
            all_urls = await _collect_all_pages_urls(
                page=page,
                max_pages=max_pages,
                per_page_scrolls=per_page_scrolls,
            )
            _dbg(f"collect → total unique job URLs: {len(all_urls)}")

            # Resolve precise titles from detail pages
            jobs: List[Job] = []
            for idx, u in enumerate(all_urls, 1):
                title = await _resolve_title_from_detail(context, u, nav_timeout_ms)
                _dbg(f"detail[{idx}/{len(all_urls)}] title={title!r} url={u}")
                try:
                    jobs.append(Job(title=title, link=u))
                except TypeError:
                    jobs.append(Job(title=title, url=u))

            await context.close()
            await browser.close()
            _dbg(f"done → jobs={len(jobs)}")
            return jobs


# ---------------- Internals ----------------

async def _ensure_results_ready(page: Page, nav_timeout_ms: int) -> None:
    """
    Make the target listing reliably ready in headless mode.
    Strategy:
      1) wait for 'networkidle' once (SPA boot)
      2) nudge scroll
      3) wait for any of:
         - anchors to job details
         - pagination text 'Page X of Y'
    """
    try:
        await page.wait_for_load_state("networkidle", timeout=min(10000, nav_timeout_ms))
    except Exception:
        pass  # not all pages reach networkidle reliably

    # small nudge + short wait
    try:
        await page.mouse.wheel(0, 2000)
        await page.wait_for_timeout(400)
    except Exception:
        pass

    selectors = [
        "a[href^='/jobs/']",
        "a[href*='https://www.metacareers.com/jobs/']",
        "a[href*='https://www.facebookcareers.com/jobs/']",
        "div:has-text('Page ')",  # 'Page X of Y'
    ]
    # Try each selector briefly; if none hit, do a couple of scroll attempts
    for sel in selectors:
        try:
            await page.wait_for_selector(sel, timeout=3000)
            _dbg(f"ready: saw {sel!r}")
            return
        except Exception:
            continue

    # Fallback: scroll a bit and retry once more
    try:
        for _ in range(2):
            await page.mouse.wheel(0, 16000)
            await page.wait_for_timeout(500)
            for sel in selectors:
                try:
                    await page.wait_for_selector(sel, timeout=2000)
                    _dbg(f"ready (fallback): saw {sel!r}")
                    return
                except Exception:
                    continue
    except Exception:
        pass
    _dbg("ready: no explicit marker seen (continuing anyway)")


async def _warmup_session(page: Page, base_origin: str, nav_timeout_ms: int) -> None:
    home = f"{base_origin}/"
    try:
        _dbg(f"warmup → {home}")
        await page.goto(home, wait_until="domcontentloaded", timeout=nav_timeout_ms)
    except PlaywrightTimeoutError:
        _dbg("warmup: homepage nav timeout (continuing)")
        return

    await _accept_cookies(page, first_time=True)

    for sel in (
        "a:has-text('View jobs')",
        "a:has-text('View Jobs')",
        "button:has-text('View jobs')",
        "button:has-text('View Jobs')",
        "a:has-text('Find jobs')",
        "button:has-text('Find jobs')",
    ):
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible():
                _dbg(f"warmup click → {sel}")
                try:
                    await loc.click(timeout=2500)
                except Exception:
                    await loc.click(timeout=2500, force=True)
                break
        except Exception:
            pass

    try:
        if "/jobs" not in page.url:
            _dbg("warmup → direct /jobs")
            await page.goto(urljoin(base_origin, "/jobs"), wait_until="domcontentloaded", timeout=nav_timeout_ms)
    except PlaywrightTimeoutError:
        _dbg("warmup: /jobs nav timeout (continuing)")


async def _accept_cookies(page: Page, first_time: bool) -> None:
    timeout_ms = 8000 if first_time else 1200
    for sel in (
        "button:has-text('Allow all')",
        "button:has-text('Accept all')",
        "button:has-text('Accept All')",
        "button[title='Allow all cookies']",
        "button:has-text('I agree')",
        "[data-cookiebanner] button:has-text('Accept')",
        "button:has-text('Accept')",
    ):
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible():
                try:
                    await loc.click(timeout=timeout_ms)
                except Exception:
                    await loc.click(timeout=timeout_ms, force=True)
                _dbg(f"cookies → clicked {sel} (first_time={first_time})")
                break
        except Exception:
            continue


async def _collect_job_urls_on_page(page: Page) -> Set[str]:
    try:
        urls = await page.eval_on_selector_all(
            "a[href^='/jobs/'], a[href*='https://www.metacareers.com/jobs/'], a[href*='https://www.facebookcareers.com/jobs/']",
            """
            els => Array.from(new Set(
              els.map(a => a.getAttribute('href') || '')
                 .map(h => h.startsWith('http') ? h : new URL(h, location.origin).toString())
                 .filter(u => /\\/jobs\\/[^/?#]+$/.test(u))
            ))
            """,
        )
        return set(urls or [])
    except Exception:
        return set()


async def _collect_all_pages_urls(page: Page, max_pages: int, per_page_scrolls: int) -> List[str]:
    seen: Set[str] = set()
    visited = 0

    while True:
        visited += 1
        _dbg(f"page[{visited}] gather…")
        inner_no_prog = 0
        prev = len(seen)
        for _ in range(max(1, per_page_scrolls)):
            urls = await _collect_job_urls_on_page(page)
            seen |= urls
            _dbg(f"  gather pass: found={len(urls)} total={len(seen)}")
            if len(seen) == prev:
                inner_no_prog += 1
            else:
                inner_no_prog = 0
                prev = len(seen)
            if inner_no_prog >= 2:
                break
            try:
                btn = page.locator(
                    "button:has-text('See more'), button:has-text('Load more'), button:has-text('Show more')"
                ).first
                if await btn.count() > 0 and await btn.is_visible():
                    try:
                        await btn.click(timeout=1000)
                    except Exception:
                        await btn.click(timeout=1000, force=True)
                    _dbg("  clicked: See/Load/Show more")
            except Exception:
                pass
            await page.mouse.wheel(0, 16000)
            await page.wait_for_timeout(350)

        cur, total, raw = await _get_pagination_info(page)
        _dbg(f"pagination text={raw!r}")
        if total is None:
            break
        if cur is not None and total is not None and cur >= total:
            break
        if visited >= max_pages:
            _dbg(f"max_pages reached ({max_pages})")
            break

        before_text = raw or ""
        before_urls = set(seen)
        if not await _click_next(page):
            _dbg("Next not clickable/visible; stop")
            break
        changed = await _wait_page_change(page, before_text, before_urls)
        _dbg(f"next → changed={changed}")
        if not changed:
            break

    return list(seen)


async def _get_pagination_info(page: Page) -> Tuple[Optional[int], Optional[int], str]:
    try:
        for el in await page.query_selector_all("div:has-text('Page ')"):
            txt = (await el.inner_text() or "").strip()
            if "Page" in txt and "of" in txt:
                m = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", txt, re.I)
                if m:
                    return int(m.group(1)), int(m.group(2)), txt
                return None, None, txt
    except Exception:
        pass
    return None, None, ""


async def _click_next(page: Page) -> bool:
    try:
        btn = page.locator("a:has-text('Next')").first
        if await btn.count() == 0 or not await btn.is_visible():
            return False
        aria = await btn.get_attribute("aria-disabled")
        if aria in ("true", "disabled"):
            return False
        try:
            await btn.click(timeout=1200)
        except Exception:
            await btn.click(timeout=1200, force=True)
        return True
    except Exception:
        return False


async def _wait_page_change(page: Page, before_text: str, before_urls: Set[str]) -> bool:
    try:
        await page.wait_for_function(
            """prev => {
                const el = Array.from(document.querySelectorAll("div"))
                  .find(d => /Page\\s+\\d+\\s+of\\s+\\d+/i.test(d.innerText || ""));
                return el && (el.innerText || "").trim() !== prev;
            }""",
            arg=before_text,
            timeout=6000,
        )
        return True
    except Exception:
        pass
    for _ in range(10):
        try:
            cur = await _collect_job_urls_on_page(page)
            if cur - before_urls:
                return True
        except Exception:
            pass
        await page.wait_for_timeout(300)
    return False


def _clean_title(t: str) -> str:
    t = (t or "").strip()
    if t.endswith(" - Meta"):
        t = t[:-7].strip()
    if t.lower() in {"find your role", "job openings at meta | meta careers"}:
        return ""
    return t


async def _title_from_jsonld(page: Page) -> str:
    try:
        scripts = await page.query_selector_all("script[type='application/ld+json']")
        for s in scripts:
            raw = await s.inner_text()
            data = json.loads(raw)
            items = data if isinstance(data, list) else [data]
            for it in items:
                if isinstance(it, dict):
                    t = (it.get("title") or it.get("name") or "").strip()
                    if t:
                        return t
    except Exception:
        pass
    return ""


async def _resolve_title_from_detail(context: BrowserContext, url: str, nav_timeout_ms: int) -> str:
    page = await context.new_page()
    try:
        try:
            _dbg(f"detail nav → {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
        except PlaywrightTimeoutError:
            _dbg("detail: nav timeout")
            return ""

        await _accept_cookies(page, first_time=False)

        try:
            await page.wait_for_selector("div[class*='_army'], h1, div[role='heading']", timeout=15000)
        except Exception:
            pass

        for sel in ("div[class*='_army']", "div._army", "div[role='heading']", "h1", "h2"):
            try:
                el = await page.query_selector(sel)
                if el:
                    t = _clean_title(await el.inner_text())
                    if t:
                        _dbg(f"detail title via {sel}: {t!r}")
                        return t
            except Exception:
                continue

        t = _clean_title(await _title_from_jsonld(page))
        if t:
            _dbg("detail title via JSON-LD")
            return t

        try:
            t = await page.eval_on_selector("meta[property='og:title']", "el => el && el.content || ''")
            t = _clean_title(t)
            if t:
                _dbg("detail title via og:title")
                return t
        except Exception:
            pass

        try:
            t = _clean_title(await page.title())
            _dbg("detail title via document.title")
            return t
        except Exception:
            return ""
    finally:
        await page.close()
