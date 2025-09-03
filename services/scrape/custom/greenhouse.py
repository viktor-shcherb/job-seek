from __future__ import annotations

import html
import re
from typing import List, Optional
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from data.model import Job
from ..url import canonical_job_url, USER_AGENT
from ..render_client import fetch_rendered_html


# Hosts like:
#   - job-boards.greenhouse.io/<org>
#   - boards.greenhouse.io/<org>
_GREENHOUSE_HOST_RE = re.compile(r"(?i)(?:^|\.)(?:job-boards\.greenhouse\.io|boards\.greenhouse\.io)$")

# Common job detail path patterns, e.g.:
#   /snyk/jobs/8071417002
#   /isomorphiclabs/jobs/5460704004
# Also allow other board formats that still contain "/jobs/<digits>".
_GH_JOB_PATH_RE = re.compile(r"/jobs/(\d+)(?:/|$)")

def _select_job_anchors(soup: BeautifulSoup):
    # Greenhouse company boards render rows like:
    # <tr class="job-post"><td class="cell"><a href=".../org/jobs/1234567890"> ... </a></td></tr>
    return soup.select('tr.job-post td.cell a[href]')

def _normalize_job_url(href: str, base_url: str) -> Optional[str]:
    """
    Make absolute, ensure greenhouse host, ensure path contains /jobs/<digits>,
    and strip query/fragment for canonicalization.
    """
    if not href:
        return None
    href = html.unescape(href)
    abs_url = href if href.startswith("http") else urljoin(base_url, href)

    p = urlparse(abs_url)
    host = (p.netloc or "").split(":")[0].lower()
    if not _GREENHOUSE_HOST_RE.search(host):
        return None
    if not _GH_JOB_PATH_RE.search(p.path):
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

def _extract_job_id_from_path(path: str) -> Optional[str]:
    m = _GH_JOB_PATH_RE.search(path or "")
    return m.group(1) if m else None


class GreenhouseAdapter:
    """
    Adapter for Greenhouse job boards.

    - Finds job detail links on listing pages (anchors inside <tr class="job-post">)
    - Follows each link and uses the first <h1> as the job title
    """
    pattern = _GREENHOUSE_HOST_RE
    name = "greenhouse"
    renders = True  # Greenhouse boards may hydrate client-side; use rendered HTML.

    @staticmethod
    def matches(url: str) -> bool:
        p = urlparse(url)
        host = (p.netloc or "").split(":")[0].lower()
        return bool(_GREENHOUSE_HOST_RE.search(host))

    @staticmethod
    async def scrape(url: str, *, timeout: int = 20, max_pages: int = 1) -> List[Job]:
        # Greenhouse boards typically list everything on one page (with filters),
        # so we load once and parse all visible postings.
        jobs: List[Job] = []
        seen_ids: set[str] = set()
        seen_links: set[str] = set()

        # 1) Load listing page and wait until job rows are present
        listing_html = await fetch_rendered_html(
            url,
            timeout_ms=timeout * 1000,
            wait_for='tr.job-post a[href*="/jobs/"]',
            user_agent=USER_AGENT,
        )
        soup = BeautifulSoup(listing_html, "html.parser")

        # 2) Collect candidate job links
        anchors = _select_job_anchors(soup)
        if not anchors:
            return jobs

        for a in anchors:
            raw_href = a.get("href")
            normalized = _normalize_job_url(raw_href, base_url=url)
            if not normalized:
                continue

            p = urlparse(normalized)
            job_id = _extract_job_id_from_path(p.path)
            link = canonical_job_url(normalized)

            # Deduplicate by job id if possible, otherwise by link
            if job_id and job_id in seen_ids:
                continue
            if not job_id and link in seen_links:
                continue

            # 3) Follow job link and extract first <h1> as title
            details_html = await fetch_rendered_html(
                link,
                timeout_ms=timeout * 1000,
                # Greenhouse job pages are SSR but play safe and wait for <h1>
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
