# services/scrape/ats/greenhouse.py
from __future__ import annotations

import re
from typing import List
from urllib.parse import urlparse

from data.model import Job
from ..http_client import get_http
from ..url import canonical_job_url

# Match:
# - boards.greenhouse.io
# - job-boards.greenhouse.io
# - boards.eu.greenhouse.io
# - job-boards.eu.greenhouse.io
_GH_HOST_RE = re.compile(r"(?:^|\.)(?:job-)?boards(?:\.eu)?\.greenhouse\.io$", re.I)


class GreenhouseAdapter:
    pattern = _GH_HOST_RE
    renders = False
    name = "greenhouse"

    @staticmethod
    def matches(url: str) -> bool:
        return bool(_GH_HOST_RE.search(urlparse(url).netloc))

    @staticmethod
    async def scrape(url: str, *, timeout: int = 20, max_pages: int = 5) -> List[Job]:
        """
        Use Greenhouse Job Board API:
          - US: https://boards-api.greenhouse.io/v1/boards/{company}/jobs
          - EU: https://boards-api-eu.greenhouse.io/v1/boards/{company}/jobs

        Fallbacks (legacy):
          - https://boards.greenhouse.io/{company}.json
          - https://job-boards.eu.greenhouse.io/{company}.json
          - https://job-boards.greenhouse.io/{company}.json
        """
        p = urlparse(url)
        segs = [s for s in p.path.split("/") if s]
        company = segs[0] if segs else None
        if not company:
            return []

        host = p.netloc.lower()
        is_eu = ".eu." in host  # e.g., job-boards.eu.greenhouse.io

        # Preferred API endpoints
        api_hosts = [
            f"https://boards-api{'-eu' if is_eu else ''}.greenhouse.io/v1/boards/{company}/jobs"
        ]

        # Legacy fallbacks (some boards still expose these)
        fallback_hosts = [
            f"https://boards.greenhouse.io/{company}.json",
            f"https://job-boards.eu.greenhouse.io/{company}.json",
            f"https://job-boards.greenhouse.io/{company}.json",
        ]

        http = await get_http()

        data = None
        for endpoint in api_hosts + fallback_hosts:
            try:
                data = await http.fetch_json(endpoint)
                if data:
                    break
            except Exception:
                # Try next endpoint
                continue

        if not data:
            return []

        jobs: List[Job] = []
        for j in (data.get("jobs") or []):
            title = (j.get("title") or "").strip()
            link = (j.get("absolute_url") or "").strip()
            if not title or not link:
                continue
            jobs.append(Job(title=title, link=canonical_job_url(link)))

        return jobs
