#!/usr/bin/env python3
"""
Scrape Meta Careers listings with Playwright and print: <title>\t<link>

Includes:
  • Warm-up: homepage → accept cookies → “View jobs” to set session cookie.
  • Robust collection of job detail URLs.
  • Pagination: clicks “Next” and merges all pages (uses "Page X of Y" text or content change).
  • Accurate titles from each detail page: div._army → div[role='heading'] → h1/h2 → JSON-LD → og:title.
  • Verbose diagnostics.

Usage:
  PYTHONPATH=. python scripts/debug_render_extract.py \
    "https://www.metacareers.com/jobs?offices[0]=Geneva%2C%20Switzerland&offices[1]=Zurich%2C%20Switzerland" \
    --headed -v --storage meta_state.json

Output: TSV lines  <title>\t<link>
"""

from __future__ import annotations
import argparse
import os
import re
import sys
import json
from contextlib import suppress
from typing import List, Dict, Optional, Tuple, Set
from urllib.parse import urljoin
from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Page,
    BrowserContext,
)

DEFAULT_MAX_SCROLLS = 40  # per-page scrolling passes


def _log(verbose: bool, msg: str) -> None:
    if verbose:
        print(msg, file=sys.stderr)


def _console_listener_factory(verbose: bool):
    def _listener(msg):
        if not verbose:
            return
        with suppress(Exception):
            t_attr = getattr(msg, "type", None)
            t = t_attr() if callable(t_attr) else (t_attr or "")
        with suppress(Exception):
            tx_attr = getattr(msg, "text", None)
            tx = tx_attr() if callable(tx_attr) else (tx_attr or "")
        _log(True, f"[console:{t}] {tx}")
    return _listener


def _wait_cookie_banner_gone(page: Page, verbose: bool) -> None:
    for sel in [
        "[data-testid='cookie-policy-dialog']",
        "div[role='dialog'] :has-text('cookies')",
        "div._3qw",
    ]:
        with suppress(Exception):
            page.locator(sel).first.wait_for(state="detached", timeout=2500)
            _log(verbose, f"[cookies] Banner {sel} detached")


def _try_accept_cookies(page: Page, verbose: bool) -> None:
    selectors = [
        "button:has-text('Allow all')",
        "button:has-text('Accept all')",
        "button:has-text('Accept All')",
        "button[title='Allow all cookies']",
        "button:has-text('I agree')",
        "[data-cookiebanner] button:has-text('Accept')",
        "button:has-text('Accept')",
    ]
    clicked = False
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc and loc.is_visible():
                loc.click(timeout=2000)
                _log(verbose, f"[cookies] Clicked '{sel}' on main page")
                clicked = True
                break
        except Exception as e:
            _log(verbose, f"[cookies] Main selector failed {sel}: {e}")
    if not clicked:
        with suppress(Exception):
            for frame in page.frames:
                for sel in selectors:
                    with suppress(Exception):
                        loc = frame.locator(sel).first
                        if loc and loc.is_visible():
                            loc.click(timeout=2000)
                            _log(verbose, f"[cookies] Clicked '{sel}' in iframe {frame.url}")
                            clicked = True
                            break
                if clicked:
                    break
    if clicked:
        _wait_cookie_banner_gone(page, verbose)
    else:
        _log(verbose, "[cookies] No cookie banner clicked (may not be shown)")


def _load_more_if_present(page: Page, verbose: bool) -> bool:
    for label in ["See more", "Load more", "Show more", "See More Jobs", "Load More Jobs"]:
        try:
            btn = page.locator(f"button:has-text('{label}')").first
            if btn and btn.is_visible():
                btn.click(timeout=1500)
                _log(verbose, f"[load-more] Clicked '{label}'")
                return True
        except Exception as e:
            _log(verbose, f"[load-more] '{label}' not clickable: {e}")
    return False


def _debug_snapshot(page: Page, verbose: bool, stage: str) -> None:
    if not verbose:
        return
    with suppress(Exception):
        _log(True, f"[snapshot:{stage}] url={page.url}")
    with suppress(Exception):
        _log(True, f"[snapshot:{stage}] title={page.title()}")
    with suppress(Exception):
        _log(True, f"[snapshot:{stage}] html_len={len(page.content())}")
    for sel in [
        "a[href^='/jobs/']",
        "a[href*='/jobs?']",
        "div:has(a[href^='/jobs/'])",
        "div[role='heading']",
        "div:has-text('Software Engineer')",
        "div:has-text('Page ')",
        "a:has-text('Next')",
    ]:
        with suppress(Exception):
            cnt = len(page.query_selector_all(sel))
            _log(True, f"[snapshot:{stage}] count({sel!r}) = {cnt}")
    with suppress(Exception):
        frames = page.frames
        _log(True, f"[snapshot:{stage}] frames={len(frames)}")
        for f in frames[:6]:
            _log(True, f"  - frame url={f.url}")


def _warmup_session(page: Page, verbose: bool, homepage: str = "https://www.metacareers.com/") -> None:
    _log(verbose, f"[warmup] Navigating to {homepage}")
    try:
        page.goto(homepage, wait_until="domcontentloaded", timeout=45000)
    except PlaywrightTimeoutError:
        _log(verbose, "[warmup] Initial homepage load timed out; proceeding")
    _try_accept_cookies(page, verbose)

    labels = [
        "View jobs", "View Jobs", "Find jobs", "Explore roles",
        "See jobs", "See all jobs", "Search jobs",
    ]
    clicked = False
    for lab in labels:
        for sel in (f"a:has-text('{lab}')", f"button:has-text('{lab}')", f"[role='link']:has-text('{lab}')"):
            try:
                loc = page.locator(sel).first
                if loc and loc.is_visible():
                    _log(verbose, f"[warmup] Clicking {sel}")
                    with suppress(Exception):
                        loc.click(timeout=2500)
                    if not (page.url.endswith("/jobs") or "/jobs" in page.url):
                        with suppress(Exception):
                            loc.click(timeout=2500, force=True)
                    clicked = True
                    break
            except Exception as e:
                _log(verbose, f"[warmup] {sel} not clickable: {e}")
        if clicked:
            break

    if not clicked:
        _log(verbose, "[warmup] Could not find 'View jobs'; going to /jobs directly")
        with suppress(Exception):
            page.goto("https://www.metacareers.com/jobs/", wait_until="domcontentloaded", timeout=30000)

    with suppress(Exception):
        page.wait_for_url("**/jobs**", timeout=15000)
    _debug_snapshot(page, verbose, stage="warmup-done")


def _collect_job_urls(page: Page, verbose: bool) -> List[str]:
    try:
        urls: List[str] = page.eval_on_selector_all(
            "a[href^='/jobs/'], a[href*='https://www.metacareers.com/jobs/']",
            """
            els => Array.from(new Set(
              els.map(a => a.getAttribute('href') || '')
                 .map(h => h.startsWith('http') ? h : new URL(h, 'https://www.metacareers.com').toString())
                 .filter(u => /\\/jobs\\/[^/?#]+$/.test(u))
            ))
            """,
        )
    except Exception as e:
        _log(verbose, f"[collect] eval failed: {e}")
        urls = []
    _log(verbose, f"[collect] found {len(urls)} unique job URLs (this page)")
    return urls


# ---------------- Pagination helpers ----------------

_PAGERE = re.compile(r"Page\s+(\d+)\s+of\s+(\d+)", re.IGNORECASE)


def _get_pagination_text(page: Page) -> str:
    with suppress(Exception):
        for el in page.query_selector_all("div:has-text('Page ')"):
            txt = (el.inner_text() or "").strip()
            if "Page" in txt and "of" in txt:
                return txt
    return ""


def _get_pagination_info(page: Page) -> Tuple[Optional[int], Optional[int], str]:
    """Return (current, total, raw_text)."""
    raw = _get_pagination_text(page)
    if not raw:
        return None, None, ""
    m = _PAGERE.search(raw)
    if not m:
        return None, None, raw
    cur, total = int(m.group(1)), int(m.group(2))
    return cur, total, raw


def _click_next(page: Page, verbose: bool) -> bool:
    """Click the Next control if available and (likely) enabled."""
    try:
        btn = page.locator("a:has-text('Next')").first
        if not btn or not btn.is_visible():
            _log(verbose, "[pagination] Next button not visible")
            return False
        # Some UIs disable via aria-disabled or pointer-events
        with suppress(Exception):
            if btn.get_attribute("aria-disabled") in ("true", "disabled"):
                _log(verbose, "[pagination] Next button aria-disabled")
                return False
        # Try normal click, then force
        try:
            btn.click(timeout=2000)
        except Exception as e:
            _log(verbose, f"[pagination] Next click needed force: {e}")
            btn.click(timeout=2000, force=True)
        _log(verbose, "[pagination] Clicked Next")
        return True
    except Exception as e:
        _log(verbose, f"[pagination] Next not clickable: {e}")
        return False


def _collect_all_pages_urls(page: Page, verbose: bool, max_pages: int, per_page_scrolls: int) -> List[str]:
    """
    Iterate pages using 'Next' until Page X of Y reaches the end or max_pages is hit.
    On each page, scroll a bit and collect job URLs.
    """
    seen: Set[str] = set()

    def _gather_within_page():
        # Scroll+gather loop per page
        inner_no_prog = 0
        prev_len = len(seen)
        for _ in range(max(1, per_page_scrolls)):
            for u in _collect_job_urls(page, verbose):
                seen.add(u)
            if len(seen) == prev_len:
                inner_no_prog += 1
            else:
                inner_no_prog = 0
                prev_len = len(seen)
            if inner_no_prog >= 2:
                break
            _load_more_if_present(page, verbose)
            page.mouse.wheel(0, 16000)
            page.wait_for_timeout(600)

    # First page
    cur, total, raw = _get_pagination_info(page)
    if total:
        _log(verbose, f"[pagination] {raw}")
    else:
        _log(verbose, "[pagination] No 'Page X of Y' text found (might be single page)")

    visited_pages = 0
    while True:
        visited_pages += 1
        _gather_within_page()

        # Decide whether to go to next page
        cur, total, raw = _get_pagination_info(page)
        if total:
            _log(verbose, f"[pagination] After gather: {raw}")
            if cur is not None and total is not None and cur >= total:
                _log(verbose, "[pagination] Reached last page")
                break
            if max_pages and visited_pages >= max_pages:
                _log(verbose, f"[pagination] Stopping at max_pages={max_pages}")
                break
        else:
            # No pagination text at all; assume single page
            break

        # Click Next and wait for page index or content to change
        before_txt = raw
        before_urls = set(seen)
        if not _click_next(page, verbose):
            _log(verbose, "[pagination] Next not clickable; stopping")
            break

        changed = False
        try:
            page.wait_for_function(
                """prev => {
                    const el = Array.from(document.querySelectorAll("div"))
                      .find(d => /Page\\s+\\d+\\s+of\\s+\\d+/i.test(d.innerText || ""));
                    return el && (el.innerText || "").trim() !== prev;
                }""",
                arg=before_txt,
                timeout=8000,
            )
            changed = True
        except Exception:
            # Fallback: wait for URL set to change
            page.wait_for_timeout(600)
            for _ in range(10):
                cur_urls = set(_collect_job_urls(page, verbose))
                if cur_urls - before_urls:
                    changed = True
                    break
                page.wait_for_timeout(400)

        if not changed:
            _log(verbose, "[pagination] Could not confirm page change; stopping to avoid loop")
            break

    return list(seen)


# ---------------- Title resolution (detail pages) ----------------

def _clean_title(t: str) -> str:
    t = (t or "").strip()
    if t.endswith(" - Meta"):
        t = t[:-7].strip()
    if t.lower() in {"find your role", "job openings at meta | meta careers"}:
        return ""
    return t


def _title_from_jsonld(page: Page) -> str:
    with suppress(Exception):
        scripts = page.query_selector_all("script[type='application/ld+json']")
        for s in scripts:
            with suppress(Exception):
                raw = s.inner_text()
                data = json.loads(raw)
                items = data if isinstance(data, list) else [data]
                for it in items:
                    if isinstance(it, dict):
                        t = (it.get("title") or it.get("name") or "").strip()
                        if t:
                            return t
    return ""


def _resolve_title_from_detail(context: BrowserContext, url: str, verbose: bool) -> str:
    page = context.new_page()
    title = ""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=35000)
    except PlaywrightTimeoutError:
        _log(verbose, f"[detail] Timeout loading {url}")
    _try_accept_cookies(page, verbose)

    with suppress(Exception):
        page.wait_for_selector("div[class*='_army'], h1, div[role='heading']", timeout=15000)

    for sel in ["div[class*='_army']", "div._army", "div[role='heading']", "h1", "h2"]:
        with suppress(Exception):
            el = page.query_selector(sel)
            if el:
                t = el.inner_text().strip()
                t = _clean_title(t)
                if t:
                    title = t
                    break

    if not title:
        t = _clean_title(_title_from_jsonld(page))
        if t:
            title = t

    if not title:
        with suppress(Exception):
            t = page.eval_on_selector("meta[property='og:title']", "el => el && el.content || ''")
            t = _clean_title(t)
            if t:
                title = t

    if not title:
        with suppress(Exception):
            title = _clean_title(page.title())

    page.close()
    return title


def _extract_jobs_across_pages(
    page: Page,
    context: BrowserContext,
    base_url: str,
    verbose: bool,
    max_pages: int,
    per_page_scrolls: int,
) -> List[Dict]:
    urls = _collect_all_pages_urls(page, verbose, max_pages=max_pages, per_page_scrolls=per_page_scrolls)
    if not urls:
        return []
    out = []
    for u in urls:
        try:
            t = _resolve_title_from_detail(context, u, verbose)
        except Exception as e:
            _log(verbose, f"[detail] Failed {u}: {e}")
            t = ""
        out.append({"title": t, "url": u if u.startswith("http") else urljoin(base_url, u)})
    return out


# ---------------- Main ----------------

def main() -> int:
    global DEFAULT_MAX_SCROLLS

    ap = argparse.ArgumentParser(description="Scrape Meta Careers titles + links (warm-up, pagination, detail title resolution).")
    ap.add_argument("url", help="Meta Careers jobs URL (e.g., https://www.metacareers.com/jobs?offices[0]=...)")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--headed", action="store_true", help="Run with browser UI")
    g.add_argument("--headless", action="store_true", help="Run headless (default)")
    ap.add_argument("--max-scrolls", type=int, default=DEFAULT_MAX_SCROLLS, help="Per-page scroll passes to gather items (default: %(default)s)")
    ap.add_argument("--max-pages", type=int, default=50, help="Maximum pages to traverse with Next (safety cap)")
    ap.add_argument("-v", "--verbose", action="store_true", help="Verbose diagnostics to stderr")
    ap.add_argument("--dump-html", metavar="PATH", help="Write listing-page HTML snapshot to PATH when verbose or on failure")
    ap.add_argument("--screenshot", metavar="PATH", help="Write listing-page screenshot to PATH when verbose or on failure")
    ap.add_argument("--storage", metavar="STATE.json", help="Load/save storage state (cookies). If file exists it's loaded; otherwise saved after warm-up.)")
    ap.add_argument("--no-warmup", action="store_true", help="Skip homepage warm-up (use if your storage already has cookies)")
    args = ap.parse_args()

    url = args.url
    DEFAULT_MAX_SCROLLS = args.max_scrolls

    _log(args.verbose, f"[start] Launching Chromium headless={not args.headed}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headed)
        ctx_kwargs = dict(
            viewport={"width": 1440, "height": 1600},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        if args.storage and os.path.exists(args.storage):
            _log(args.verbose, f"[state] Loading storage state from {args.storage}")
            ctx_kwargs["storage_state"] = args.storage

        context = browser.new_context(**ctx_kwargs)
        page = context.new_page()

        if args.verbose:
            page.on("console", _console_listener_factory(True))

        if not args.no_warmup and ("storage_state" not in ctx_kwargs):
            _warmup_session(page, args.verbose)
            if args.storage:
                with suppress(Exception):
                    context.storage_state(path=args.storage)
                    _log(args.verbose, f"[state] Saved storage state to {args.storage}")

        # Navigate to the target listing
        try:
            _log(args.verbose, f"[nav] Goto target URL {url}")
            resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
            if resp is not None:
                _log(args.verbose, f"[nav] status={resp.status} url={resp.url}")
        except PlaywrightTimeoutError:
            print("Failed to load page", file=sys.stderr)
            context.close()
            browser.close()
            return 2

        _try_accept_cookies(page, args.verbose)
        _debug_snapshot(page, args.verbose, stage="post-target")

        if ("login" in page.url) or ("checkpoint" in page.url):
            _log(args.verbose, f"[auth] Login wall at {page.url}; attempting warm-up + retry")
            _warmup_session(page, args.verbose)
            with suppress(Exception):
                page.goto(url, wait_until="domcontentloaded", timeout=45000)
                _debug_snapshot(page, args.verbose, stage="post-retry")

        # Ensure some results are present
        with suppress(Exception):
            page.wait_for_selector("div:has(a[href^='/jobs/'])", timeout=20000)

        jobs = _extract_jobs_across_pages(
            page=page,
            context=context,
            base_url=url,
            verbose=args.verbose,
            max_pages=args.max_pages,
            per_page_scrolls=args.max_scrolls,
        )

        if args.verbose and args.dump_html:
            with suppress(Exception):
                with open(args.dump_html, "w", encoding="utf-8") as f:
                    f.write(page.content())
                _log(True, f"[dump] Wrote HTML to {args.dump_html}")
        if args.verbose and args.screenshot:
            with suppress(Exception):
                page.screenshot(path=args.screenshot, full_page=True)
                _log(True, f"[dump] Wrote screenshot to {args.screenshot}")

        context.close()
        browser.close()

    # TSV output
    for job in jobs:
        title = (job["title"] or "").strip().replace("\t", " ")
        link = job["url"].strip()
        print(f"{title}\t{link}")

    _log(args.verbose, f"[done] Extracted {len(jobs)} jobs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
