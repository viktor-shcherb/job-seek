# services/scrape/extractors/repeated_blocks.py
from __future__ import annotations
from typing import List, Set, Tuple, Dict

from data.model import Job
from services.scrape.normalization import (
    _is_generic_classkey,
    _selector_from_key,
    _looks_like_job_detail_url,
    _max_heading_text,
    _title_from_attrs,
    _clean_anchor_text,
)
from services.scrape.url import _absolute, canonical_job_url


def extract_repeated_block_jobs(soup, base_url: str, *, min_children: int = 3) -> List[Job]:
    """
    1) Walk containers and find child groups where many direct children share same (tag, class).
    2) Treat each such (tag, class) as an 'item prototype'.
    3) Query the whole document for ALL elements matching that prototype.
    4) From each item, pick detail link + title (max heading -> ARIA/title -> anchor text).
    """
    jobs: List[Job] = []
    seen_links: Set[str] = set()
    candidate_keys: Set[Tuple[str, str]] = set()

    # --- Step 1/2: discover item prototypes
    for container in soup.find_all(["div", "section", "main", "article"]):
        groups: Dict[Tuple[str, str], List] = {}
        for child in container.find_all(recursive=False):
            tag = child.name or ""
            cls_key = " ".join(sorted(child.get("class", [])))
            if not tag or _is_generic_classkey(cls_key):
                continue
            key = (tag, cls_key)
            groups.setdefault(key, []).append(child)

        for key, children in groups.items():
            if len(children) >= min_children:
                candidate_keys.add(key)

    if not candidate_keys:
        return []

    # --- Step 3/4: scrape ALL items of each candidate class across the whole doc
    for key in candidate_keys:
        selector = _selector_from_key(key)
        for item in soup.select(selector):
            a = (
                item.select_one('a[data-automation-id="jobTitle"][href]') or
                item.select_one("a.posting-title[href]") or
                item.find("a", href=True)
            )
            if not a:
                continue

            try:
                link = _absolute(a.get("href", ""), base_url)
                if not _looks_like_job_detail_url(link):
                    continue
                link = canonical_job_url(link)
            except Exception:
                continue

            title = _max_heading_text(item) or _title_from_attrs(a) or _clean_anchor_text(a)
            if not title:
                title = _max_heading_text(a)
            if not title:
                continue

            if link in seen_links:
                continue
            seen_links.add(link)
            jobs.append(Job(title=title, link=link))

    return jobs
