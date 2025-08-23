# services/scrape/http_client.py
from __future__ import annotations
import atexit, asyncio, ssl
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import aiohttp, certifi
from .url import USER_AGENT

# Detect Brotli availability (prefer brotlicffi; fall back to Brotli)
try:
    import brotlicffi as _brotli  # noqa: F401
    _BROTLI_OK = True
except Exception:
    try:
        import brotli as _brotli  # noqa: F401
        _BROTLI_OK = True
    except Exception:
        _BROTLI_OK = False

_DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br" if _BROTLI_OK else "gzip, deflate",
    "Upgrade-Insecure-Requests": "1",
}

_SESSION: Optional[aiohttp.ClientSession] = None

class HttpClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def fetch_text(self, url: str, *, params: Dict[str, Any] | None = None,
                         headers: Dict[str, str] | None = None) -> str:
        return await self._get_text_with_fallbacks(url, params=params, headers=headers)

    async def fetch_json(self, url: str, *, params: Dict[str, Any] | None = None,
                         json: Any | None = None, headers: Dict[str, str] | None = None,
                         method: str = "GET") -> Any:
        hdrs = dict(_DEFAULT_HEADERS)
        if headers:
            hdrs.update(headers)
        if method.upper() == "GET":
            async with self.session.get(url, params=params, headers=hdrs) as r:
                r.raise_for_status()
                return await r.json(content_type=None)
        else:
            async with self.session.post(url, params=params, json=json, headers=hdrs) as r:
                r.raise_for_status()
                return await r.json(content_type=None)

    async def post_json(
            self,
            url: str,
            *,
            params: Dict[str, Any] | None = None,
            json: Any | None = None,
            headers: Dict[str, str] | None = None,
    ) -> Any:
        return await self.fetch_json(
            url,
            params=params,
            json=json,
            headers=headers,
            method="POST",
        )

    async def _get_text_with_fallbacks(self, url: str, *, params=None, headers=None) -> str:
        """
        - For metacareers, first try without 'br' (they're picky).
        - On 400/403/406/451, retry once with simplified headers and no 'br'.
        """
        hdrs = dict(_DEFAULT_HEADERS)
        if headers:
            hdrs.update(headers)

        host = urlparse(url).netloc.lower()

        def _no_br(h: Dict[str, str]) -> Dict[str, str]:
            h2 = dict(h)
            h2["Accept-Encoding"] = "gzip, deflate"
            return h2

        # First attempt
        first_headers = hdrs
        if "metacareers.com" in host:
            # Be conservative with encoding for Meta
            first_headers = _no_br(first_headers)
            first_headers["Accept"] = "text/html,application/xhtml+xml,*/*;q=0.8"
            first_headers.setdefault("Referer", "https://www.metacareers.com/")

        try:
            async with self.session.get(url, params=params, headers=first_headers) as r:
                r.raise_for_status()
                return await r.text()
        except aiohttp.ClientResponseError as e:
            status = getattr(e, "status", 0) or 0
            if status in (400, 403, 406, 451):
                retry_headers = _no_br({
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Upgrade-Insecure-Requests": "1",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                    "Referer": f"{urlparse(url).scheme}://{host}/",
                })
                async with self.session.get(url, params=params, headers=retry_headers) as r2:
                    r2.raise_for_status()
                    return await r2.text()
            raise  # bubble up other errors

async def get_http() -> HttpClient:
    global _SESSION
    if _SESSION is None or _SESSION.closed:
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit=20, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=45)
        _SESSION = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            raise_for_status=False,  # let methods decide when to raise
            auto_decompress=True,
            trust_env=True,
        )
        # graceful shutdown
        def _close():
            if _SESSION and not _SESSION.closed:
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        loop.create_task(_SESSION.close())
                    else:
                        loop.run_until_complete(_SESSION.close())
                except Exception:
                    pass
        atexit.register(_close)
    return HttpClient(_SESSION)
