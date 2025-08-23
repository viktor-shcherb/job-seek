# services/scrape/headless.py
from __future__ import annotations

import asyncio
from typing import Optional

from playwright.async_api import async_playwright, Browser, BrowserContext

_BROWSER: Optional[Browser] = None
_CONTEXT: Optional[BrowserContext] = None
_PLAYWRIGHT = None
_LOCK = asyncio.Lock()

USER_AGENT = (
    # a fairly standard desktop UA string
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

async def get_browser_context() -> BrowserContext:
    """
    Singleton-ish browser context for all headless scrapes.
    Keeps a single Chromium instance + context alive.
    """
    global _PLAYWRIGHT, _BROWSER, _CONTEXT
    async with _LOCK:
        if _CONTEXT and not _CONTEXT.is_closed():
            return _CONTEXT

        if _BROWSER and not _BROWSER.is_connected():
            _BROWSER = None

        if _PLAYWRIGHT is None:
            _PLAYWRIGHT = await async_playwright().start()

        if _BROWSER is None:
            _BROWSER = await _PLAYWRIGHT.chromium.launch(
                headless=True,
                args=[
                    # keep things simple & less fingerprinty
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )

        # fresh context with useful defaults (no persistent storage)
        _CONTEXT = await _BROWSER.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 900},
            locale="en-US",
            java_script_enabled=True,
            # Playwright handles Brotli automatically; no custom encodings needed
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        return _CONTEXT
