from __future__ import annotations

import re
import unicodedata
from typing import Iterable, List, Optional
from urllib.parse import urlparse, urlunparse, urljoin

from bs4 import BeautifulSoup

from data.model import Job
from ..url import canonical_job_url, USER_AGENT
from ..render_client import fetch_rendered_html


# Strictly match Proton's board on Greenhouse EU
_PROTON_HOST_RE = re.compile(r"(^|\.)job-boards\.eu\.greenhouse\.io$", re.IGNORECASE)


def _strip_accents(s: str) -> str:
    if not s:
        return ""
    # Fold accents -> ASCII where possible
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


def _norm(s: str) -> str:
    # Case/accents-insensitive, collapse non-alnum to single space
    s = _strip_accents(s).casefold()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()


def _split_locations(loc_text: str) -> List[str]:
    """
    Greenhouse renders locations like: "Geneva; Taipei; Paris; "
    We split on common separators and strip empties.
    """
    if not loc_text:
        return []
    parts = re.split(r"[;,/|•·]+", loc_text)
    out = []
    for p in parts:
        t = p.strip()
        if t:
            out.append(t)
    return out


def _damerau_levenshtein_capped(a: str, b: str, max_dist: int = 2) -> int:
    """
    Damerau-Levenshtein with early exit when distance exceeds max_dist.
    This is a simple banded implementation suitable for short strings.
    """
    # Quick length bound
    if abs(len(a) - len(b)) > max_dist:
        return max_dist + 1

    # Ensure a is the shorter
    if len(a) > len(b):
        a, b = b, a

    # Initialize the three rows
    prev_prev = list(range(len(b) + 1))
    prev = [0] * (len(b) + 1)
    curr = [0] * (len(b) + 1)

    for i, ca in enumerate(a, start=1):
        curr[0] = i
        # band limits
        min_j = max(1, i - max_dist)
        max_j = min(len(b), i + max_dist)

        # Pre-fill outside the band with big numbers to keep pruning effective
        for j in range(1, min_j):
            curr[j] = max_dist + 1
        for j in range(max_j + 1, len(b) + 1):
            curr[j] = max_dist + 1

        best_row_val = max_dist + 1

        for j in range(min_j, max_j + 1):
            cb = b[j - 1]
            cost = 0 if ca == cb else 1

            # substitutions / insertions / deletions
            curr[j] = min(
                prev[j] + 1,          # deletion
                curr[j - 1] + 1,      # insertion
                prev[j - 1] + cost    # substitution
            )

            # transposition
            if i > 1 and j > 1 and ca == b[j - 2] and a[i - 2] == cb:
                curr[j] = min(curr[j], prev_prev[j - 2] + 1)

            if curr[j] < best_row_val:
                best_row_val = curr[j]

        if best_row_val > max_dist:
            return max_dist + 1

        # roll rows
        prev_prev, prev, curr = prev, curr, prev_prev

    dist = prev[len(b)]
    return dist


def _any_fuzzy_match(candidates: Iterable[str], terms: Iterable[str], *, max_edit_distance: int) -> bool:
    """
    True if any candidate location matches any search term:
      - substring match (normalized) OR
      - Damerau-Levenshtein distance <= max_edit_distance
    """
    norm_cands = [ _norm(c) for c in candidates ]
    norm_terms = [ _norm(t) for t in terms if t and _norm(t) ]

    if not norm_terms:
        # No filters configured -> accept everything
        return True

    for c in norm_cands:
        if not c:
            continue
        for t in norm_terms:
            if not t:
                continue
            # quick substring acceptance either way
            if t in c or c in t:
                return True
            # fuzzy edit distance (cap)
            if _damerau_levenshtein_capped(t, c, max_edit_distance) <= max_edit_distance:
                return True
    return False


class ProtonAdapter:
    """
    Scraper for Proton's Greenhouse board: https://job-boards.eu.greenhouse.io/proton

    Configuration:
      - location_terms: list of substrings to match against job location(s)
      - max_edit_distance: up to how many character edits (incl. swaps) to allow for fuzzy matches
    """
    pattern = _PROTON_HOST_RE
    name = "proton"
    renders = True

    def __init__(self, location_terms: Optional[List[str]] = None, *, max_edit_distance: int = 2) -> None:
        self.location_terms: List[str] = location_terms or []
        self.max_edit_distance: int = max_edit_distance

    @staticmethod
    def matches(url: str) -> bool:
        p = urlparse(url)
        host = p.netloc.split(":")[0].lower()
        if not _PROTON_HOST_RE.search(host):
            return False
        # constrain to the Proton board
        path = (p.path or "/").rstrip("/")
        return path == "/proton" or path.startswith("/proton/")

    async def scrape(self, url: str, *, timeout: int = 20, max_pages: int = 5) -> List[Job]:
        """
        Fetch the Proton board page, collect job rows, and filter by fuzzy locations if configured.
        """
        # Ensure we land on the list page (if they gave a job detail URL)
        p = urlparse(url)
        base_list_path = "/proton"
        list_url = urlunparse((p.scheme or "https", p.netloc, base_list_path, "", "", ""))

        html = await fetch_rendered_html(
            list_url,
            timeout_ms=timeout * 1000,
            # the rows exist server-side; wait for table rows just in case
            wait_for=".job-posts--table--department tr.job-post a[href]",
            user_agent=USER_AGENT,
        )

        soup = BeautifulSoup(html, "html.parser")
        anchors = soup.select(".job-posts--table--department tr.job-post a[href]")

        jobs: List[Job] = []

        for a in anchors:
            href = a.get("href")
            if not href:
                continue

            # Title: remove the "New" pill if present
            title_tag = a.select_one(".body.body--medium")
            if title_tag:
                # Drop the badge container to avoid 'New' leaking into title
                for pill in title_tag.select(".tag-container"):
                    pill.decompose()
                title = title_tag.get_text(" ", strip=True)
            else:
                # fallback to link text
                title = a.get_text(" ", strip=True)

            # Locations
            loc_tag = a.select_one(".body.body__secondary.body--metadata")
            loc_text = loc_tag.get_text(" ", strip=True) if loc_tag else ""
            locs = _split_locations(loc_text)

            # Fuzzy location filter (if configured)
            if not _any_fuzzy_match(locs, self.location_terms, max_edit_distance=self.max_edit_distance):
                continue

            # Absolute + canonical URL
            abs_link = href if href.startswith("http") else urljoin(list_url, href)
            link = canonical_job_url(abs_link)

            if title and link:
                jobs.append(Job(title=title, link=link))

        return jobs
