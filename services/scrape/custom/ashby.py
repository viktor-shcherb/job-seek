from __future__ import annotations

import html
import re
from typing import List, Optional
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from data.model import Job
from ..url import canonical_job_url, USER_AGENT
from ..render_client import fetch_rendered_html


# Host: jobs.ashbyhq.com
_ASHBY_HOST_RE = re.compile(r"(?i)(?:^|\.)(?:jobs\.ashbyhq\.com)$")

# UUID in the path after org slug: /<org>/<uuid>
_UUID_RE = re.compile(
    r"/[^/]+/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})(?:/|$)"
)

def _get_org_slug(url: str) -> Optional[str]:
    p = urlparse(url)
    # path like "/lakera.ai/" -> org is first non-empty segment
    segments = [s for s in p.path.split("/") if s]
    return segments[0] if segments else None

def _select_job_anchors(soup: BeautifulSoup, org: Optional[str]):
    """
    Ashby renders each job as an <a> wrapping the tile. Classes are hashed, so rely on href shape.
    Prefer anchors that start with "/<org>/" and contain '-' (UUID has dashes).
    """
    if org:
        sel = f'a[href^="/{org}/"][href*="-"]'
        anchors = soup.select(sel)
        if anchors:
            return anchors
    # Fallback: any anchor with a UUID-looking segment in path
    return [a for a in soup.select("a[href]") if _UUID_RE.search(urlparse(a["href"]).path or "")]

def _normalize_job_url(href: str, base_url: str) -> Optional[str]:
    """
    Make absolute, verify Ashby host and presence of UUID in path,
    strip query/fragment for canonicalization.
    """
    if not href:
        return None
    href = html.unescape(href)
    abs_url = href if href.startswith("http") else urljoin(base_url, href)

    p = urlparse(abs_url)
    host = (p.netloc or "").split(":")[0].lower()
    if not _ASHBY_HOST_RE.search(host):
        return None
    if not _UUID_RE.search(p.path or ""):
        return None

    cleaned = urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), p.params, "", ""))
    return cleaned

def _extract_title_from_h1(html_text: str) -> Optional[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    h1 = soup.find("h1")
    if not h1:
        return None
    t = h1.get_text(" ", strip=True)
    return t or None

def _extract_uuid(path: str) -> Optional[str]:
    m = _UUID_RE.search(path or "")
    return m.group(1) if m else None


class AshbyAdapter:
    """
    Adapter for Ashby boards on jobs.ashbyhq.com.

    - Collects job links shaped like /<org>/<uuid>
    - Follows each link; first <h1> is the job title
    """
    pattern = _ASHBY_HOST_RE
    name = "ashbyhq"
    renders = True  # Ashby boards are hydrated client-side

    @staticmethod
    def matches(url: str) -> bool:
        p = urlparse(url)
        host = (p.netloc or "").split(":")[0].lower()
        return bool(_ASHBY_HOST_RE.search(host))

    @staticmethod
    async def scrape(url: str, *, timeout: int = 20, max_pages: int = 1) -> List[Job]:
        jobs: List[Job] = []
        seen_ids: set[str] = set()
        seen_links: set[str] = set()

        org = _get_org_slug(url)

        # 1) Load listing page and wait until job tiles are present.
        #    We wait for anchors that point at "/<org>/" and contain '-' (UUID dashes).
        listing_wait_for = f'a[href^="/{org}/"][href*="-"]' if org else 'a[href*="-"]'
        listing_html = await fetch_rendered_html(
            url,
            timeout_ms=timeout * 1000,
            wait_for=listing_wait_for,
            user_agent=USER_AGENT,
        )
        soup = BeautifulSoup(listing_html, "html.parser")

        # 2) Collect candidate job links
        anchors = _select_job_anchors(soup, org)
        if not anchors:
            return jobs

        for a in anchors:
            raw_href = a.get("href")
            normalized = _normalize_job_url(raw_href, base_url=url)
            if not normalized:
                continue

            p = urlparse(normalized)
            job_id = _extract_uuid(p.path)
            link = canonical_job_url(normalized)

            # Deduplicate by UUID if present, otherwise by link
            if job_id and job_id in seen_ids:
                continue
            if not job_id and link in seen_links:
                continue

            # 3) Follow detail page and extract title from first <h1>
            details_html = await fetch_rendered_html(
                link,
                timeout_ms=timeout * 1000,
                wait_for="h1",
                user_agent=USER_AGENT,
            )
            title = _extract_title_from_h1(details_html)
            if not title:
                continue

            jobs.append(Job(title=title, link=link))
            if job_id:
                seen_ids.add(job_id)
            else:
                seen_links.add(link)

        return jobs
