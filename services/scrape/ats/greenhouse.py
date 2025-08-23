# services/scrape/ats/greenhouse.py
from __future__ import annotations

import re
from typing import List
from urllib.parse import urlparse

from data.model import Job
from ..http_client import get_http
from ..url import canonical_job_url

_GH_HOST_RE = re.compile(r"(?:^|\.)boards\.greenhouse\.io$", re.I)


class GreenhouseAdapter:
    pattern = _GH_HOST_RE

    @staticmethod
    def matches(url: str) -> bool:
        return bool(_GH_HOST_RE.search(urlparse(url).netloc))

    @staticmethod
    async def scrape(url: str, *, timeout: int = 20, max_pages: int = 5) -> List[Job]:
        """
        Greenhouse offers JSON at:
          https://boards.greenhouse.io/{company}.json
        """
        p = urlparse(url)
        segs = [s for s in p.path.split("/") if s]
        company = segs[0] if segs else None
        if not company:
            return []

        api = f"https://boards.greenhouse.io/{company}.json"
        http = await get_http()
        data = await http.fetch_json(api)

        jobs: List[Job] = []
        for j in data.get("jobs", []):
            title = (j.get("title") or "").strip()
            link = (j.get("absolute_url") or "").strip()
            if not title or not link:
                continue
            jobs.append(Job(title=title, link=canonical_job_url(link)))
        return jobs
