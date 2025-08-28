from __future__ import annotations

import html
import re
from typing import List, Optional
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode, quote

from bs4 import BeautifulSoup

from data.model import Job
from ..url import canonical_job_url, USER_AGENT
from ..render_client import fetch_rendered_html


# Hosts like:
#   - <tenant>.wd5.myworkdayjobs.com
#   - wd1.myworkdaysite.com
_WORKDAY_HOST_RE = re.compile(
    r"(?i)(?:^|\.)(?:[\w-]+\.wd\d+\.myworkdayjobs\.com|wd\d+\.myworkdaysite\.com)$"
)

# JR / R / REQ ids (optionally with -1 etc., and optional separator)
_REQ_ID_RE = re.compile(r"\b((?:JR|R|REQ)[-_]?\d{4,8}(?:-\d+)?)\b", re.IGNORECASE)


def _build_page_url(base_url: str, page: int) -> str:
    """
    Preserve all existing query parameters and set page=<page>.
    """
    p = urlparse(base_url)
    q = parse_qs(p.query, keep_blank_values=True)
    q["page"] = [str(page)]
    query = urlencode({k: v for k, v in q.items()}, doseq=True, quote_via=quote)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, query, p.fragment))


def _select_job_links(soup: BeautifulSoup):
    # Workday job cards consistently include data-automation-id="jobTitle"
    return soup.select('a[data-automation-id="jobTitle"][href]')


def _extract_title(a) -> Optional[str]:
    t = a.get_text(" ", strip=True)
    return t or None


def _extract_req_id(text: str) -> Optional[str]:
    m = _REQ_ID_RE.search(text or "")
    return m.group(1) if m else None


def _to_details_url(abs_url: str) -> str:
    """
    Convert listing URL forms like:
      /en-US/<app>/job/<Location>/Title_REQ?...  -->  /en-US/<app>/details/Title_REQ?...
    If already a details URL, return as-is.
    """
    p = urlparse(abs_url)
    segments = [s for s in p.path.split("/") if s != ""]  # keep order, drop empties for easier ops

    # If already contains 'details', leave it
    if "details" in segments:
        return abs_url

    # Replace 'job/<location>' with 'details'
    try:
        idx = segments.index("job")
        if idx + 1 < len(segments):
            new_segments = segments[:idx] + ["details"] + segments[idx + 2 :]
            new_path = "/" + "/".join(new_segments)
            return urlunparse((p.scheme, p.netloc, new_path, p.params, p.query, p.fragment))
    except ValueError:
        pass  # no 'job' segment; fall through

    # Fallback: if no 'job' marker, just return original
    return abs_url


class WorkdayAdapter:
    """
    Generic adapter for Workday recruiting boards:

      - myworkdayjobs.com (e.g., <tenant>.wd5.myworkdayjobs.com/SomeApp)
      - myworkdaysite.com (e.g., wd1.myworkdaysite.com/en-US/recruiting/<org>/<tenant>/jobs)

    It extracts job title + link from list pages and canonicalizes links to '/details/...'.
    """
    pattern = _WORKDAY_HOST_RE
    name = "workday-generic"
    renders = True  # Workday listings often hydrate client-side

    @staticmethod
    def matches(url: str) -> bool:
        p = urlparse(url)
        host = (p.netloc or "").split(":")[0].lower()
        return bool(_WORKDAY_HOST_RE.search(host))

    @staticmethod
    async def scrape(url: str, *, timeout: int = 20, max_pages: int = 5) -> List[Job]:
        jobs: List[Job] = []
        seen_req_ids: set[str] = set()
        seen_links: set[str] = set()

        parsed = urlparse(url)
        q = parse_qs(parsed.query)
        has_page_param = "page" in q
        try:
            start_pg = int(q.get("page", ["1"])[0] or "1")
        except Exception:
            start_pg = 1

        for i in range(max_pages):
            pg = start_pg + i
            page_url = url if (i == 0 and not has_page_param) else _build_page_url(url, pg)

            html_text = await fetch_rendered_html(
                page_url,
                timeout_ms=timeout * 1000,
                wait_for='a[data-automation-id="jobTitle"][href]',
                user_agent=USER_AGENT,
            )
            soup = BeautifulSoup(html_text, "html.parser")
            links = _select_job_links(soup)

            if not links:
                break

            page_added = 0
            for a in links:
                raw_href = a.get("href")
                if not raw_href:
                    continue

                # Unescape &amp; and make absolute
                abs_href = html.unescape(raw_href)
                abs_url = abs_href if abs_href.startswith("http") else urljoin(page_url, abs_href)

                # Canonicalize to '/details/...'
                details_url = _to_details_url(abs_url)
                link = canonical_job_url(details_url)

                title = _extract_title(a)
                if not (title and link):
                    continue

                # Deduplicate by Req ID when available, else by link
                rid = _extract_req_id(link) or _extract_req_id(title) or _extract_req_id(abs_url)
                if rid and rid in seen_req_ids:
                    continue
                if not rid and link in seen_links:
                    continue

                jobs.append(Job(title=title, link=link))
                if rid:
                    seen_req_ids.add(rid)
                else:
                    seen_links.add(link)
                page_added += 1

            if page_added == 0:
                break

        return jobs
