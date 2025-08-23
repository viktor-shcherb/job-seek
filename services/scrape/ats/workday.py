# services/scrape/ats/workday.py
from __future__ import annotations

import re
from typing import Dict, List
from urllib.parse import urljoin, urlparse, parse_qsl

from data.model import Job
from ..url import USER_AGENT, canonical_job_url
from ..http_client import get_http


_LOCALE_RE = re.compile(r"^[a-z]{2}-[A-Z]{2}$")
_HOST_RE = re.compile(r"(^|\.)(?:wd\d+\.)?myworkdayjobs\.com$", re.I)


class WorkdayAdapter:
    pattern = _HOST_RE

    @staticmethod
    def matches(url: str) -> bool:
        return bool(_HOST_RE.search(urlparse(url).netloc))

    @staticmethod
    def _site_parts(url: str) -> tuple[str | None, str | None, str | None]:
        """
        Return (host, tenant, career_site) or (None, None, None).
        Examples:
          - nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite
          - nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite
          - myworkdayjobs.com/{tenant}/{careerSite}
          - myworkdayjobs.com/{locale}/{tenant}/{careerSite}
        """
        p = urlparse(url)
        host = p.netloc
        segs = [s for s in p.path.split("/") if s]

        # tenant from subdomain like "nvidia.wd5.myworkdayjobs.com"
        m = re.match(r"^([^.]+)\.wd\d+\.myworkdayjobs\.com$", host, flags=re.I)
        tenant = m.group(1) if m else None

        # locale + careerSite from path
        i = 0
        locale = segs[i] if (len(segs) > i and _LOCALE_RE.match(segs[i])) else None
        if locale:
            i += 1
        if tenant:
            career_site = segs[i] if len(segs) > i else None
        else:
            tenant = segs[i] if len(segs) > i else None
            career_site = segs[i + 1] if len(segs) > i + 1 else None

        if not (host and tenant and career_site):
            return None, None, None
        return host, tenant, career_site

    @staticmethod
    async def scrape(url: str, *, timeout: int = 20, max_pages: int = 5) -> List[Job]:
        host, tenant, career_site = WorkdayAdapter._site_parts(url)
        if not host:
            return []

        endpoint = f"https://{host}/wday/cxs/{tenant}/{career_site}/jobs"

        # Collect applied facets from the query string (repeatable)
        q_pairs = parse_qsl(urlparse(url).query, keep_blank_values=True)
        applied: Dict[str, List[str]] = {}
        for k, v in q_pairs:
            kl = k.lower()
            if kl in {
                "locations", "location", "locationhierarchy1", "locationhierarchy2",
                "locationcity", "locationstate", "timetype", "workersubtype",
                "jobfamilygroup", "jobfamily", "category",
            } and v:
                applied.setdefault(kl, []).append(v)

        headers_override = {"User-Agent": USER_AGENT, "Accept": "application/json", "Content-Type": "application/json"}

        limit = 20
        offset = 0
        seen: Dict[str, Job] = {}

        for _ in range(max_pages):
            payload = {"appliedFacets": applied, "limit": limit, "offset": offset, "searchText": ""}
            http = await get_http()
            data = await http.post_json(endpoint, json=payload)
            postings = data.get("jobPostings") or []
            if not postings:
                break

            for p in postings:
                title = (p.get("title") or p.get("titleSimple") or "").strip()
                path = (p.get("externalPath") or p.get("canonicalPositionUrl") or "").strip()
                if not title or not path:
                    continue
                link = canonical_job_url(urljoin(f"https://{host}", path))
                if link not in seen:
                    seen[link] = Job(title=title, link=link)

            total = data.get("total") or data.get("totalFound")
            offset += limit
            if total is not None:
                try:
                    if offset >= int(total):
                        break
                except Exception:
                    pass

        return list(seen.values())
