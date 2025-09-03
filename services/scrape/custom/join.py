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
#   - join.com
#   - www.join.com
_JOIN_HOST_RE = re.compile(r"(?i)(?:^|\.)(?:join\.com)$")

# Job details URLs look like: /companies/<org>/<digits>-<slug>
_JOIN_JOB_PATH_RE = re.compile(r"^/companies/[^/]+/\d{5,}-[A-Za-z0-9-]+/?$")


def _select_job_anchors(soup: BeautifulSoup):
    # Cards render as <a data-testid="Link" href="https://join.com/companies/.../<id>-<slug>">
    return soup.select('a[data-testid="Link"][href]')


def _normalize_job_url(href: str, base_url: str) -> Optional[str]:
    """
    Return absolute, cleaned job details URL if it matches the expected join.com pattern.
    Strip query + fragment to canonicalize before passing to canonical_job_url.
    """
    if not href:
        return None

    href = html.unescape(href)
    abs_url = href if href.startswith("http") else urljoin(base_url, href)

    p = urlparse(abs_url)
    host = (p.netloc or "").split(":")[0].lower()
    if not _JOIN_HOST_RE.search(host):
        return None
    if not _JOIN_JOB_PATH_RE.match(p.path):
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


class JoinAdapter:
    """
    Adapter for join.com company listings.

    - Finds job detail links on listing pages (href like /companies/<org>/<id>-<slug>)
    - Follows each link and takes the first <h1> text as the job title
    """
    pattern = _JOIN_HOST_RE
    name = "join.com"
    renders = True  # listings and details are hydrated client-side

    @staticmethod
    def matches(url: str) -> bool:
        p = urlparse(url)
        host = (p.netloc or "").split(":")[0].lower()
        return bool(_JOIN_HOST_RE.search(host))

    @staticmethod
    async def scrape(url: str, *, timeout: int = 20, max_pages: int = 1) -> List[Job]:
        # Note: join.com uses infinite scroll; this implementation scrapes the initial rendered page.
        jobs: List[Job] = []
        seen_links: set[str] = set()

        # 1) Load the listing page and wait for at least one job link to be present
        listing_html = await fetch_rendered_html(
            url,
            timeout_ms=timeout * 1000,
            # Wait for rendered job tiles; we filter to /companies/... links afterwards
            wait_for='a[data-testid="Link"][href*="/companies/"][href*="-"]',
            user_agent=USER_AGENT,
        )
        soup = BeautifulSoup(listing_html, "html.parser")

        # 2) Collect candidate job links
        for a in _select_job_anchors(soup):
            raw_href = a.get("href")
            normalized = _normalize_job_url(raw_href, base_url=url)
            if not normalized:
                continue

            link = canonical_job_url(normalized)
            if link in seen_links:
                continue

            # 3) Follow each job link, wait for <h1>, and extract title
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
            seen_links.add(link)

        return jobs
