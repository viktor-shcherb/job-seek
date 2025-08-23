# services/scrape/extractors/jsonld.py
from __future__ import annotations

import json
from typing import List, Dict, Any

from bs4 import BeautifulSoup

from data.model import Job
from services.scrape.normalization import _looks_like_job_detail_url
from services.scrape.url import _absolute, canonical_job_url


def _iter_nodes(payload: Any) -> List[Dict[str, Any]]:
    """
    Normalize LD+JSON payloads into a flat list of dict nodes.
    Handles dict, list, and @graph; also follows common "mainEntity" wrappers.
    """
    nodes: List[Dict[str, Any]] = []

    def add(node: Any) -> None:
        if isinstance(node, dict):
            nodes.append(node)
            # common nesting
            if isinstance(node.get("@graph"), list):
                for g in node["@graph"]:
                    add(g)
            if isinstance(node.get("mainEntity"), dict):
                add(node["mainEntity"])
            if isinstance(node.get("item"), dict):
                add(node["item"])
        elif isinstance(node, list):
            for n in node:
                add(n)

    add(payload)
    return nodes


def extract_jsonld_jobs(soup: BeautifulSoup, base_url: str) -> List[Job]:
    jobs: List[Job] = []

    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        raw = tag.string or tag.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue

        try:
            data = json.loads(raw)
        except Exception:
            # Some LD+JSON are minified but malformed (trailing commas, etc.).
            # Skip quietly; other extractors will catch jobs.
            continue

        for node in _iter_nodes(data):
            t = node.get("@type")
            if t == "JobPosting" or (isinstance(t, list) and "JobPosting" in t):
                title = (node.get("title") or node.get("name") or "").strip()
                raw_url = (node.get("url") or node.get("applicationUrl") or "").strip()
                if not title or not raw_url:
                    continue
                url_abs = _absolute(raw_url, base_url)
                if _looks_like_job_detail_url(url_abs):
                    jobs.append(Job(title=title, link=canonical_job_url(url_abs)))

    return jobs
