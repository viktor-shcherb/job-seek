# services/scrape/extractors/__init__.py
from __future__ import annotations
from typing import List
from bs4 import BeautifulSoup

from data.model import Job
from .jsonld import extract_jsonld_jobs
from .listitem import extract_listitem_jobs
from .repeated_blocks import extract_repeated_block_jobs
from .anchor import extract_anchor_jobs_strict

# Highest signal first; early exit on first non-empty result.
EXTRACTOR_PIPELINE = (
    extract_jsonld_jobs,
    extract_listitem_jobs,
    extract_repeated_block_jobs,
    extract_anchor_jobs_strict,
)

def extract_all(soup: BeautifulSoup, base_url: str) -> List[Job]:
    for fn in EXTRACTOR_PIPELINE:
        print(f"Extracting {base_url} with {fn.__name__}")
        jobs = fn(soup, base_url)  # type: ignore[arg-type]
        if jobs:
            return jobs
    return []
