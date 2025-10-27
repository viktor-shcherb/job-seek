# services/scrape/extractors/listitem.py
from __future__ import annotations
from typing import List

from bs4 import BeautifulSoup

from data.model import Job
from services.scrape.normalization import (
    _looks_like_job_detail_url,
    _max_heading_text,
    _title_from_aria,
    _clean_anchor_text,
)
from services.scrape.url import _absolute, canonical_job_url


def extract_listitem_jobs(soup: BeautifulSoup, base_url: str) -> List[Job]:
    jobs: List[Job] = []
    seen: set[str] = set()

    def list_is_job_list(ul_or_ol) -> bool:
        label = (ul_or_ol.get("aria-label") or "").lower()
        if any(k in label for k in ("job", "career", "vacan", "opening", "position")):
            return True

        # Workday signature: several jobTitle anchors inside this list
        if len(ul_or_ol.select('a[data-automation-id="jobTitle"][href]')) >= 2:
            return True

        # Generic heuristic fallback
        count = 0
        for a in ul_or_ol.find_all("a", href=True):
            try:
                href_abs = _absolute(a.get("href", ""), base_url)
            except Exception:
                continue

            if _looks_like_job_detail_url(href_abs):
                count += 1
                if count >= 2:
                    return True
        return False

    candidate_lists = [l for l in soup.find_all(["ul", "ol"]) if list_is_job_list(l)]

    li_iterables = (
        [el for L in candidate_lists for el in L.select('li, div[role="listitem"]')]
        if candidate_lists
        else soup.select('li, div[role="listitem"]')
    )
    print(f"[listitem] found {len(li_iterables)} list items")

    for li in li_iterables:
        chosen_a = li.select_one('a[data-automation-id="jobTitle"][href]')
        link_abs = None

        if chosen_a:
            try:
                link_abs = _absolute(chosen_a.get("href", ""), base_url)
            except Exception:
                link_abs = None
        else:
            # fallback: first anchor that looks like a job detail
            for cand in li.find_all("a", href=True):
                try:
                    href_abs = _absolute(cand.get("href", ""), base_url)
                except Exception:
                    continue

                if _looks_like_job_detail_url(href_abs):
                    chosen_a, link_abs = cand, href_abs
                    break

        if not chosen_a or not link_abs:
            continue

        try:
            link_abs = canonical_job_url(link_abs)
        except Exception:
            continue

        if link_abs in seen:
            continue

        title = _max_heading_text(li) or _title_from_aria(chosen_a) or _clean_anchor_text(chosen_a)
        if not title:
            # Try any other anchor in the same li
            for other in li.find_all("a", href=True):
                t2 = _title_from_aria(other) or _clean_anchor_text(other)
                if t2.strip():
                    title = t2
                    break

            # Try spans in the same li
            for other in li.find_all("span"):
                classes = other.get("class", [])
                if isinstance(classes, str):
                    classes = [classes]
                if any('title' in _class for _class in classes):
                    title = other.text
                    if title.strip():
                        break

        print(link_abs, title)
        if not title:
            continue

        seen.add(link_abs)
        jobs.append(Job(title=title.strip(), link=link_abs))

    return jobs
