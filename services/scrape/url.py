# services/scrape/url.py
from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse, parse_qsl, urlunparse, urlencode

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)


def _absolute(url: str, base: str) -> str:
    """
    Resolve possibly-relative `url` against `base` and return an absolute URL.
    """
    return urljoin(base, url)


def _is_http_url(href: str | None) -> bool:
    if not href or href.startswith(("mailto:", "tel:", "javascript:")):
        return False
    parsed = urlparse(href)
    return parsed.scheme in ("http", "https") or (not parsed.scheme and bool(parsed.path))


# Params that must NOT affect job identity
_JOB_IGNORE_PARAMS = {
    "page", "start", "offset",              # pagination
    "ref", "referral", "src", "source",     # refs
    "gh_src", "gh_jid",                     # Greenhouse
    "_gl", "_ga", "_gac",                   # GA
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "locations", "location", "locationHierarchy1", "locationHierarchy2",
    "locationCity", "locationState", "lat", "lng",
}


def canonical_job_url(url: str) -> str:
    """
    Canonicalize a job detail URL:
      - Collapse accidental repeated segments like /jobs/results/jobs/results/
      - Drop volatile params (utm, gh_src, pagination, etc.)
      - Keep ordering of remaining params stable
    """
    p = urlparse(url)
    path = re.sub(r"/(jobs/results)(?:/\1)+", r"/\1", p.path)

    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
         if k.lower() not in _JOB_IGNORE_PARAMS]
    q.sort()
    return urlunparse((p.scheme, p.netloc, path, p.params, urlencode(q, doseq=True), p.fragment))


def normalize_page_identity(url: str) -> str:
    """
    Normalize a listing page URL for the purpose of de-duplication:
      - Remove page=1, start=0, offset=0
      - Keep a stable ordering of remaining query params
    """
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))

    if q.get("page") == "1":
        q.pop("page", None)
    for k in ("start", "offset", "from", "startrow"):
        if str(q.get(k)) in ("0",):
            q.pop(k, None)

    q_items = sorted(q.items())
    return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q_items, doseq=True), p.fragment))
