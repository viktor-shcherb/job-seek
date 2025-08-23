# services/scrape/extractors/anchor.py
from __future__ import annotations
from typing import List

from bs4 import BeautifulSoup

from data.model import Job
from services.scrape.normalization import (
    _looks_like_job_detail_url,
    _title_from_aria,
    _clean_anchor_text,
)
from services.scrape.url import _absolute, canonical_job_url


def extract_anchor_jobs_strict(soup: BeautifulSoup, base_url: str) -> List[Job]:
    jobs: List[Job] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href_abs = _absolute(a.get("href", ""), base_url)
        if (a.get("data-automation-id") == "jobTitle") or _looks_like_job_detail_url(href_abs):
            link = canonical_job_url(href_abs)
            if link in seen:
                continue
            title = _title_from_aria(a) or _clean_anchor_text(a)
            if not title:
                continue
            seen.add(link)
            jobs.append(Job(title=title, link=link))

    return jobs
