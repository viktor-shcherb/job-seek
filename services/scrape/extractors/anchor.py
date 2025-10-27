# services/scrape/extractors/anchor.py
from __future__ import annotations

import re
from typing import List
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from data.model import Job
from services.scrape.normalization import (
    _looks_like_job_detail_url,
    _title_from_aria,
    _clean_anchor_text,
)
from services.scrape.url import _absolute, canonical_job_url


_CTA_TITLES = {"view job", "learn more", "read more", "apply now", "connect"}


def _heading_text_in(node) -> str | None:
    """Return text of the highest-priority heading (h1..h6) under node."""
    for level in range(1, 7):  # prefer higher heading levels
        h = node.find(f"h{level}")
        if h:
            txt = h.get_text(" ", strip=True)
            if txt:
                return txt
    return None


def _has_meaningful_heading(a) -> bool:
    """Heuristic: anchor contains a heading that looks like a job title."""
    try:
        t = _heading_text_in(a)
        if not t:
            return False
        t_clean = t.strip()
        if len(t_clean) < 4:
            return False
        if t_clean.lower() in _CTA_TITLES:
            return False
        # Optional: reject titles that are mostly punctuation
        if sum(ch.isalnum() for ch in t_clean) < 3:
            return False
        return True
    except Exception:
        return False


def _title_from_heading(a) -> str | None:
    """Prefer headings inside the anchor: h1 > h2 > ... > h6."""
    t = _heading_text_in(a)
    return t.strip() if t else None


def extract_anchor_jobs_strict(soup: BeautifulSoup, base_url: str) -> List[Job]:
    jobs: List[Job] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        try:
            href_abs = _absolute(a.get("href", ""), base_url)

            looks_like_job = (
                a.get("data-automation-id") == "jobTitle"
                or "jobTitle" in a.get("class", [])
                or _looks_like_job_detail_url(href_abs)
                or _has_meaningful_heading(a)
            )
            if not looks_like_job:
                continue

            link = canonical_job_url(href_abs)
            if link in seen:
                continue

            title = (
                _title_from_heading(a)     # ← prefer highest-level heading
                or _title_from_aria(a)
                or _clean_anchor_text(a)
            )
            if not title:
                continue

            seen.add(link)
            jobs.append(Job(title=title, link=link))
        except Exception:
            continue

    return jobs
