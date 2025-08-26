# services/scrape/ats/__init__.py
from __future__ import annotations

import re
from typing import List, Optional, Protocol, Tuple, Type

from data.model import Job
from ..url import canonical_job_url

class ATSAdapter(Protocol):
    pattern: re.Pattern[str]
    name: str
    renders: bool

    @staticmethod
    def matches(url: str) -> bool: ...
    @staticmethod
    async def scrape(url: str, *, timeout: int = 20, max_pages: int = 5) -> List[Job]: ...

# --- concrete adapters ---
from .workday import WorkdayAdapter  # noqa: E402
from .greenhouse import GreenhouseAdapter  # noqa: E402
from .lever import LeverAdapter  # noqa: E402
from .meta import MetaCareersAdapter  # <-- NEW
from .microsoft import MicrosoftAdapter

_ADAPTERS: List[Type[ATSAdapter]] = [
    WorkdayAdapter,
    GreenhouseAdapter,
    LeverAdapter,
    MetaCareersAdapter,
    MicrosoftAdapter
]

def _first_matching_adapter(url: str) -> Optional[ATSAdapter]:
    for adapter in _ADAPTERS:
        if adapter.matches(url):
            return adapter
    return None

async def scrape_via_ats_if_supported(
    website_url: str,
    *,
    timeout: int = 20,
    max_pages: int = 5,
) -> Optional[Tuple[List[Job], str, bool]]:
    adapter = _first_matching_adapter(website_url)
    if not adapter:
        return None

    jobs = await adapter.scrape(website_url, timeout=timeout, max_pages=max_pages)
    out: List[Job] = []
    seen: set[str] = set()
    for j in jobs:
        link = canonical_job_url(str(j.link))
        if link in seen:
            continue
        seen.add(link)
        out.append(Job(title=j.title, link=link))
    return out, adapter.name, adapter.renders
