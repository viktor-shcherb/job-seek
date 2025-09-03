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


_ESRI_JOB_PATH_RE = re.compile(r"^/careers/\d{6,}$")  # numeric id


def _is_esri_job_anchor(a, href_abs: str) -> bool:
    """True only for Esri careers job cards."""
    try:
        p = urlparse(href_abs)
        if not p.netloc.endswith("esri.com"):
            return False
        if not _ESRI_JOB_PATH_RE.match(p.path or ""):
            return False
        # Extra signal from the card markup
        cls = a.get("class", [])
        return ("careers-link" in cls) or (a.get("data-component-link-type") == "card")
    except Exception:
        return False


def _esri_title_from_card(a) -> str | None:
    # Prefer the clean attribute if present
    t = a.get("data-component-link")
    if t:
        return t.strip()
    # Fallback to the H2 within the card
    h2 = a.find("h2", class_=re.compile(r"careers-title|title", re.I))
    if h2 and h2.get_text(strip=True):
        return h2.get_text(strip=True)
    return None


def extract_anchor_jobs_strict(soup: BeautifulSoup, base_url: str) -> List[Job]:
    jobs: List[Job] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        try:
            href_abs = _absolute(a.get("href", ""), base_url)

            # Broaden the predicate with Esri support
            looks_like_job = (
                a.get("data-automation-id") == "jobTitle"
                or a.get("class") == "jobTitle"
                or "jobTitle" in a.get("class", [])
                or _looks_like_job_detail_url(href_abs)
                or _is_esri_job_anchor(a, href_abs)
            )
            if not looks_like_job:
                continue

            link = canonical_job_url(href_abs)
            if link in seen:
                continue

            # Prefer clean Esri title, then your existing fallbacks
            title = _esri_title_from_card(a) or _title_from_aria(a) or _clean_anchor_text(a)
            if not title:
                continue

            seen.add(link)
            jobs.append(Job(title=title, link=link))
        except Exception:
            continue

    return jobs
