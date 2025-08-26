# services/scrape/ats/microsoft.py
from __future__ import annotations

import re
import string
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, quote

from bs4 import BeautifulSoup

from data.model import Job
from ..http_client import get_http  # not used directly here, but kept for parity with other adapters
from ..url import canonical_job_url, USER_AGENT
from ..render_client import fetch_rendered_html

# Match careers.microsoft.com and any subdomain like jobs.careers.microsoft.com
_MC_HOST_RE = re.compile(r"(^|\.)careers\.microsoft\.com$", re.IGNORECASE)


def _slugify_ms_title(title: str) -> str:
    """
    Microsoft slug rules (observed):
      - Trim
      - Replace every ASCII space U+0020 with '-'
      - Percent-encode any char not in [A-Za-z0-9-] using UTF-8 (no lowercasing)
      - Keep existing hyphens; don't collapse multiple '-'
    """
    t = (title or "").strip()
    t = t.replace(" ", "-")
    safe = string.ascii_letters + string.digits + "-"  # unescaped
    return quote(t, safe=safe)


_JOB_ITEM_ID_RE = re.compile(r"\bJob item\s+(\d{6,})\b", re.IGNORECASE)
_ANY_DIGITS_RE = re.compile(r"(\d{6,})")


def _extract_job_id(item: BeautifulSoup) -> Optional[str]:
    """
    Heuristics (prefer the most reliable source):
      1) aria-label like "Job item 1854316"
      2) Any descendant attributes containing a 6+ digit block
         - prefer shorter (6-8) over longer (e.g., IDs with suffixes)
    """
    # 1) aria-label anchor
    for el in item.select('[aria-label]'):
        m = _JOB_ITEM_ID_RE.search(el.get("aria-label", ""))
        if m:
            return m.group(1)

    # 2) scan attributes for digit blocks
    candidates: List[Tuple[int, str]] = []
    for el in item.descendants:
        if not getattr(el, "attrs", None):
            continue
        for k, v in el.attrs.items():
            if isinstance(v, (list, tuple)):
                vals = [str(x) for x in v]
            else:
                vals = [str(v)]
            for val in vals:
                for m in _ANY_DIGITS_RE.finditer(val):
                    digits = m.group(1)
                    # Prefer reasonable job id lengths first
                    score = (0 if 6 <= len(digits) <= 8 else 1, len(digits))
                    candidates.append((score[0] * 100 + score[1], digits))

    if candidates:
        # Choose the "best" candidate: prefer 6-8 digits, then shortest length
        candidates.sort(key=lambda t: t[0])
        return candidates[0][1]

    return None


def _build_page_url(base_url: str, page: int) -> str:
    """
    Preserve all query params and only update pg=<page>. Keep %20 (not '+').
    """
    p = urlparse(base_url)
    q = parse_qs(p.query, keep_blank_values=True)
    q["pg"] = [str(page)]
    query = urlencode({k: v for k, v in q.items()}, doseq=True, quote_via=quote)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, query, p.fragment))


def _select_job_items(soup: BeautifulSoup) -> Iterable:
    # Primary card containers
    return soup.select('#job-search-app [role="listitem"].ms-List-cell, div[role="listitem"].ms-List-cell')


def _extract_title(item: BeautifulSoup) -> Optional[str]:
    h2 = item.select_one("h2")
    if h2:
        return h2.get_text(" ", strip=True)
    return None


class MicrosoftAdapter:
    pattern = _MC_HOST_RE
    renders = True
    name = "microsoft"

    @staticmethod
    def matches(url: str) -> bool:
        host = urlparse(url).netloc.split(":")[0].lower()
        # Fast-path without regex to avoid subtle regex anchoring issues
        return host.endswith("careers.microsoft.com")

    @staticmethod
    async def scrape(url: str, *, timeout: int = 20, max_pages: int = 5) -> List[Job]:
        """
        Render the search page(s), extract job id + title, and compose canonical detail URLs.
        """
        jobs: List[Job] = []
        seen_ids: set[str] = set()

        # Determine starting page
        try:
            start_pg = int(parse_qs(urlparse(url).query).get("pg", ["1"])[0] or "1")
        except Exception:
            start_pg = 1

        for i in range(max_pages):
            pg = start_pg + i
            page_url = _build_page_url(url, pg)

            html = await fetch_rendered_html(
                page_url,
                timeout_ms=timeout * 1000,
                wait_for="#job-search-app [role='listitem'], [data-automationid='ListCell']",
                user_agent=USER_AGENT,
            )
            soup = BeautifulSoup(html, "html.parser")
            items = list(_select_job_items(soup))

            # If nothing rendered, stop early
            if not items:
                break

            page_added = 0
            for item in items:
                jid = _extract_job_id(item)
                title = _extract_title(item)
                if not (jid and title):
                    continue

                if jid in seen_ids:
                    continue

                slug = _slugify_ms_title(title)
                link = f"https://jobs.careers.microsoft.com/global/en/job/{jid}/{slug}"
                link = canonical_job_url(link)

                jobs.append(Job(title=title, link=link))
                seen_ids.add(jid)
                page_added += 1

            # Heuristic: if this page yielded zero new jobs, pagination likely exhausted
            if page_added == 0:
                break

        return jobs
