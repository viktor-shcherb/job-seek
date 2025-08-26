# services/scrape/render_client.py
import asyncio
from typing import Optional
from playwright.async_api import async_playwright

_pw = None
_browser = None
_lock = asyncio.Lock()

async def _ensure_browser():
    global _pw, _browser
    async with _lock:
        if _browser is None:
            _pw = await async_playwright().start()
            # chromium is broadly compatible; you can switch to firefox/webkit if needed
            _browser = await _pw.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--no-sandbox"],
            )
    return _browser

async def fetch_rendered_html(
    url: str,
    *,
    timeout_ms: int = 30_000,
    wait_for: str = '#job-search-app [role="listitem"], [data-automationid="jobCard"], [data-automation-id="job-card"]',
    user_agent: Optional[str] = None,
) -> str:
    browser = await _ensure_browser()
    context = await browser.new_context(
        user_agent=user_agent,
        java_script_enabled=True,
        locale="en-US",
        viewport={"width": 1366, "height": 900},
    )
    async def _route(route):
        # Block only heavy assets
        if route.request.resource_type in {"image", "media", "font"}:
            await route.abort()
        else:
            await route.continue_()
    await context.route("**/*", _route)

    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

        # Dismiss cookie/consent if it blocks data loading
        for sel in [
            '#onetrust-accept-btn-handler',
            'button:has-text("Accept all")',
            'button:has-text("Accept")',
            'button[aria-label="Accept"]',
            '#mscc-accept-all',
        ]:
            try:
                if await page.locator(sel).is_visible(timeout=1500):
                    await page.click(sel, timeout=1500)
                    break
            except Exception:
                pass

        # Let XHRs settle, then wait for real job nodes
        await page.wait_for_load_state("networkidle")
        try:
            await page.wait_for_selector(wait_for, timeout=timeout_ms)
        except Exception:
            # One more chance: the app sometimes renders late
            await page.wait_for_load_state("networkidle")
        return await page.content()
    finally:
        await context.close()