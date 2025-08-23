# services/scrape/ats/lever.py
from __future__ import annotations

import re
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse, parse_qs

from data.model import Job
from ..http_client import get_http
from ..url import canonical_job_url

_LEVER_HOST_RE = re.compile(r"(?:^|\.)jobs\.lever\.co$", re.I)


def _as_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v if x is not None]
    return [str(v)]


def _lower_nonempty(values: List[str]) -> List[str]:
    return [s.strip().lower() for s in values if isinstance(s, str) and s.strip()]


def _extract_filters(url: str) -> Dict[str, List[str]]:
    """Normalize known query filters from the Lever hosted page URL."""
    qs = parse_qs(urlparse(url).query, keep_blank_values=True)
    def vals(*keys: str) -> List[str]:
        out: List[str] = []
        for k in keys:
            out.extend(qs.get(k, []))
        return _lower_nonempty(out)

    return {
        "locations": vals("location", "locations"),
        "departments": vals("department", "team"),
        "commitments": vals("commitment", "employment_type", "type"),
        "search": vals("search", "query", "q"),
        # some companies encode workplace style in categories/workplaceType
        "workplace": vals("workplaceType", "workplace", "workplace_type"),
    }


def _match_any(haystacks: List[str], needles: List[str]) -> bool:
    """True iff any needle (substring, case-insensitive) matches any haystack."""
    if not needles:
        return True
    joined = " | ".join(haystacks).lower()
    return any(n in joined for n in needles)


def _posting_matches_filters(p: Dict[str, Any], f: Dict[str, List[str]]) -> bool:
    title = (p.get("text") or p.get("title") or "")  # title text
    cats = p.get("categories") or {}

    loc_vals = _as_list(cats.get("location"))
    team_vals = _as_list(cats.get("team") or cats.get("department"))
    com_vals = _as_list(cats.get("commitment"))
    wp_vals  = _as_list(p.get("workplaceType") or cats.get("workplaceType"))

    # every non-empty filter group must match
    if not _match_any([title], f["search"]):
        return False
    if not _match_any(loc_vals, f["locations"]):
        return False
    if not _match_any(team_vals, f["departments"]):
        return False
    if not _match_any(com_vals, f["commitments"]):
        return False
    if not _match_any(wp_vals, f["workplace"]):
        return False
    return True


class LeverAdapter:
    pattern = _LEVER_HOST_RE

    @staticmethod
    def matches(url: str) -> bool:
        return bool(_LEVER_HOST_RE.search(urlparse(url).netloc))

    @staticmethod
    def _company_from_url(url: str) -> Optional[str]:
        p = urlparse(url)
        segs = [s for s in p.path.split("/") if s]
        return segs[0] if segs else None

    @staticmethod
    async def scrape(url: str, *, timeout: int = 20, max_pages: int = 5) -> List[Job]:
        """
        Lever JSON (most boards):
          1) https://jobs.lever.co/{company}.json
          2) fallback: https://api.lever.co/v0/postings/{company}?mode=json

        We apply client-side filtering to respect query params from the hosted page URL
        (e.g., ?location=...&department=...).
        """
        company = LeverAdapter._company_from_url(url)
        if not company:
            return []

        filters = _extract_filters(url)
        jobs: List[Job] = []

        http = await get_http()

        # Try the hosted JSON first
        try:
            data = await http.fetch_json(f"https://jobs.lever.co/{company}.json")
            filtered = [p for p in data if _posting_matches_filters(p, filters)]
            for p in filtered:
                title = (p.get("text") or p.get("title") or "").strip()
                lever_url = (p.get("hostedUrl") or p.get("applyUrl") or p.get("url") or "").strip()
                if not title or not lever_url:
                    continue
                jobs.append(Job(title=title, link=canonical_job_url(lever_url)))
            if jobs:
                return jobs
        except Exception:
            jobs.clear()

        # Fallback to public API
        try:
            data = await http.fetch_json(
                f"https://api.lever.co/v0/postings/{company}",
                params={"mode": "json"},
            )
            filtered = [p for p in data if _posting_matches_filters(p, filters)]
            for p in filtered:
                title = (p.get("text") or p.get("title") or "").strip()
                lever_url = (
                    p.get("hostedUrl")
                    or p.get("applyUrl")
                    or (p.get("urls") or {}).get("show")
                    or ""
                ).strip()
                if not title or not lever_url:
                    continue
                jobs.append(Job(title=title, link=canonical_job_url(lever_url)))
        except Exception:
            pass

        return jobs
