# services/scrape/__init__.py
from __future__ import annotations

from typing import Dict, List, Optional, Set
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from data.model import Job
from .ats import scrape_via_ats_if_supported
from .extractors import extract_all
from .http_client import get_http
from .pagination import discover_next_page_url
from .url import USER_AGENT, canonical_job_url, normalize_page_identity


async def scrape_jobs(
    website_url: str,
    *,
    timeout: int = 20,
    max_pages: int = 5,
    same_host_only: bool = True,
) -> List[Job]:
    """
    Fully-async scraper entrypoint.
      - Uses a single shared aiohttp session (create if not provided)
      - Routes known ATS to dedicated adapters
      - Falls back to generic HTML extractors with simple pagination
    """

    try:
        # 1) ATS fast-path
        ats_jobs = await scrape_via_ats_if_supported(website_url, timeout=timeout, max_pages=max_pages)
        if ats_jobs is not None:
            return ats_jobs

        # 2) Generic HTML flow (with conservative pagination)
        visited: Set[str] = set()
        collected: Dict[str, Job] = {}
        base_host = urlparse(website_url).netloc

        url = website_url
        for _ in range(max_pages):
            url = normalize_page_identity(url)
            if url in visited:
                break
            visited.add(url)

            http = await get_http()
            html = await http.fetch_text(url)
            soup = BeautifulSoup(html, "html.parser")

            page_jobs = extract_all(soup, url)

            # merge (canonicalize link as the key)
            for j in page_jobs:
                key = canonical_job_url(str(j.link))
                if key not in collected:
                    collected[key] = Job(title=j.title, link=key)

            # pagination discovery
            next_url = discover_next_page_url(soup, url, url)
            if not next_url:
                break
            if same_host_only and urlparse(next_url).netloc and urlparse(next_url).netloc != base_host:
                break

            url = next_url

        return list(collected.values())
    finally:
        pass
