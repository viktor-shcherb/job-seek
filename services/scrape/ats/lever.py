from __future__ import annotations

import re
from typing import List, Iterable, Tuple
from urllib.parse import urlparse, parse_qs
import httpx

from data.model import Job


_LEVER_HOST_RE = re.compile(r"^(?:www\.)?jobs(?:\.eu)?\.lever\.co(?::\d+)?$", re.IGNORECASE)
_ALLOWED_FILTERS = {"location", "department", "team", "commitment", "level"}

def _api_host_for_jobs_host(netloc: str) -> str:
    netloc = netloc.lower()
    if netloc.endswith("jobs.eu.lever.co"):
        return "api.eu.lever.co"
    return "api.lever.co"

def _collect_filter_params(query: dict) -> Iterable[Tuple[str, str]]:
    # parse_qs yields lists for each key; keep multiple values (Lever ORs them).
    for key in _ALLOWED_FILTERS:
        for val in query.get(key, []):
            if val:
                yield (key, val)

class LeverAdapter:
    pattern = _LEVER_HOST_RE
    renders = False
    name = "lever"

    @staticmethod
    def matches(url: str) -> bool:
        from urllib.parse import urlparse
        return bool(_LEVER_HOST_RE.search(urlparse(url).netloc))

    @staticmethod
    async def scrape(url: str, *, timeout: int = 20, max_pages: int = 5) -> List[Job]:
        """
        Scrape Lever postings from a Lever-hosted jobs URL (list or detail),
        honoring UI-provided filters in the query string.

        Returns: List[Job] with title/link populated.
        """
        parsed = urlparse(url)
        netloc = parsed.netloc
        path_parts = [p for p in parsed.path.split("/") if p]
        if not path_parts:
            return []

        site = path_parts[0]
        posting_id = path_parts[1] if len(path_parts) > 1 else None

        query = parse_qs(parsed.query, keep_blank_values=False)
        base_host = _api_host_for_jobs_host(netloc)
        alt_host = "api.lever.co" if base_host.startswith("api.eu.") else "api.eu.lever.co"

        headers = {
            "Accept": "application/json",
            # Be explicit; avoid caches/ETag complications.
            "Cache-Control": "no-cache",
        }

        jobs: List[Job] = []
        async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
            # If the URL points at a specific posting, fetch just that posting.
            if posting_id:
                for host in (base_host, alt_host):
                    api_url = f"https://{host}/v0/postings/{site}/{posting_id}"
                    try:
                        resp = await client.get(api_url)
                        if resp.status_code == 404:
                            continue
                        resp.raise_for_status()
                        data = resp.json()
                        title = data.get("text")
                        link = data.get("hostedUrl")
                        if title and link:
                            jobs.append(Job(title=title.strip(), link=link))
                        return jobs
                    except httpx.HTTPError:
                        continue
                return jobs  # empty if not found

            # Otherwise fetch a paginated listing, passing through supported filters.
            common_params: list[tuple[str, str]] = [("mode", "json")]
            common_params.extend(_collect_filter_params(query))

            limit = 50
            skip = 0
            pages_fetched = 0
            host_cycle = (base_host, alt_host)
            host_idx = 0

            while pages_fetched < max_pages:
                host = host_cycle[host_idx]
                params = common_params + [("skip", str(skip)), ("limit", str(limit))]
                api_url = f"https://{host}/v0/postings/{site}"
                try:
                    resp = await client.get(api_url, params=params)
                    if resp.status_code == 404 and host_idx == 0:
                        # Try the other region once, then continue with it.
                        host_idx = 1
                        continue
                    resp.raise_for_status()
                    data = resp.json()

                    # The standard response is a list of postings.
                    postings = data.get("data") if isinstance(data, dict) else data
                    if not postings:
                        break

                    for p in postings:
                        title = p.get("text")
                        link = p.get("hostedUrl") or p.get("applyUrl")
                        if not link and p.get("id"):
                            link = f"https://jobs.lever.co/{site}/{p['id']}"
                        if title and link:
                            jobs.append(Job(title=title.strip(), link=link))

                    pages_fetched += 1
                    if len(postings) < limit:
                        break
                    skip += limit
                except httpx.HTTPError:
                    # If first host failed for some reason, flip once and retry this page;
                    # otherwise give up on further pages.
                    if host_idx == 0:
                        host_idx = 1
                        continue
                    break

        return jobs
