# services/scrape/__init__.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Set, Tuple, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from data.model import Job
from .custom import scrape_via_ats_if_supported
from .extractors import extract_all
from .http_client import get_http
from .pagination import discover_next_page_url
from .url import USER_AGENT, canonical_job_url, normalize_page_identity
from .js_detect import looks_js_shell
from .render_client import fetch_rendered_html

@dataclass
class ScrapeMeta:
    # True if we used a headless renderer at least once during this attempt.
    # None means “unknown” (e.g., ATS handled the scrape and didn’t report).
    renderer_used: Optional[bool] = None
    # How many pages we attempted vs. how many required rendering.
    attempted_pages: int = 0
    rendered_pages: int = 0
    # Optional: name of ATS adapter used (if you wire it up later)
    ats_adapter: Optional[str] = None


async def scrape_jobs_with_meta(
    website_url: str,
    *,
    timeout: int = 20,
    max_pages: int = 5,
    same_host_only: bool = True,
) -> Tuple[List[Job], ScrapeMeta]:
    meta = ScrapeMeta(renderer_used=False, attempted_pages=0, rendered_pages=0)

    try:
        # 1) ATS fast-path
        ats_jobs = await scrape_via_ats_if_supported(website_url, timeout=timeout, max_pages=max_pages)
        if ats_jobs is not None:
            # If in the future your ATS returns (jobs, meta_like), detect and merge here.
            jobs, ats, rendered = ats_jobs  # type: ignore[assignment]
            # We don't know if ATS rendered; leave renderer_used=None to mean “unknown”
            meta.renderer_used = rendered
            meta.ats_adapter = ats
            # meta.ats_adapter can be filled by your ATS layer later.
            # print(f"[scrape_jobs] ATS: {len(jobs)}")
            return list(jobs), meta  # ensure it's a list

        # 2) Generic HTML flow (with conservative pagination)
        visited: Set[str] = set()
        collected: Dict[str, Job] = {}
        base_host = urlparse(website_url).netloc

        url = website_url
        for _ in range(max_pages):
            meta.attempted_pages += 1

            try:
                url = normalize_page_identity(url)
                if url in visited:
                    break
                visited.add(url)

                http = await get_http()
                html = await http.fetch_text(url)
            except Exception:
                break

            # Fallback to headless render if page looks JS-only
            if looks_js_shell(html):
                print(f"[scrape_jobs] JS shell: {url}")
                html = await fetch_rendered_html(
                    url,
                    timeout_ms=timeout * 1000,
                    # Job-like selectors: tuned for broad sites
                    wait_for="#job-search-app [role='listitem'], [data-automationid='ListCell'], main, [role='list'], #jobs, .job, .jobs, article",
                    user_agent=USER_AGENT,
                )
                meta.rendered_pages += 1
                meta.renderer_used = True  # at least one page required rendering

            soup = BeautifulSoup(html, "html.parser")
            page_jobs = extract_all(soup, url)

            for j in page_jobs:
                try:
                    key = canonical_job_url(str(j.link))
                except Exception:
                    continue

                if key not in collected:
                    collected[key] = Job(title=j.title, link=key)

            print(f"[scrape_jobs] page discovery at {url}")
            try:
                next_url = discover_next_page_url(soup, url, url)
                if not next_url:
                    break
                if same_host_only and urlparse(next_url).netloc and urlparse(next_url).netloc != base_host:
                    break
                url = next_url
            except Exception:
                break

        return list(collected.values()), meta
    finally:
        pass
