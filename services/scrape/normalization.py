# services/scrape/normalization.py
from __future__ import annotations

import re
from typing import Tuple
from urllib.parse import urlparse

# Segments that strongly indicate a non-detail page.
BAD_PATH_SEGMENTS = {
    "saved", "alerts", "recommendations", "dashboard", "signin", "sign-in",
    "login", "help", "support", "about", "privacy", "terms", "eeo",
    "how-we-hire", "legal", "saved jobs", "saved-jobs"
}

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)

# Known ATS hosts (conservative; prefer fewer false positives).
ATS_HOST_PATTERNS = [
    re.compile(r"(?:^|\.)jobs\.lever\.co$", re.I),
    re.compile(r"(?:^|\.)boards\.greenhouse\.io$", re.I),
    re.compile(r"(?:^|\.)smartrecruiters\.com$", re.I),
    re.compile(r"(?:^|\.)workable\.com$", re.I),
    re.compile(r"(?:^|\.)jobvite\.com$", re.I),
    re.compile(r"(?:^|\.)ashbyhq\.com$", re.I),
    # Workday: bare myworkdayjobs.com OR <tenant>.wd<nn>.myworkdayjobs.com
    re.compile(r"(?:^|\.)(?:[a-z0-9-]+\.wd\d+\.)?myworkdayjobs\.com$", re.I),
]


def _host_matches_ats(host: str) -> bool:
    return any(p.search(host) for p in ATS_HOST_PATTERNS)


# URL path shapes for actual job detail pages.
JOB_DETAIL_PATTERNS = [
    # Apple-style details/apply
    re.compile(r"(^|/)(?:[a-z]{2}-[a-z]{2}/)?details/\d{6,}(?:-\d+)?(?:/|$)", re.I),
    re.compile(r"(^|/)(?:app/)?[a-z]{2}-[a-z]{2}/apply/\d{6,}(?:-\d+)?(?:/|$)", re.I),

    # Generic numeric IDs found under common sections
    re.compile(r"(^|/)jobs?/results?/\d", re.I),
    re.compile(r"(^|/)careers?/.*/\d", re.I),
    re.compile(r"(^|/)positions?/\d", re.I),
    re.compile(r"(^|/)vacanc(?:y|ies)/\d", re.I),

    # Req ID slug at end of path
    re.compile(r"(^|/)job/[^/]+/[^/]+_(?:JR|R|REQ)[-_]?\d{4,}(?:-\d+)?(?:/|$)", re.I),

    # Oracle Cloud Recruiting (careers.oracle.com) job detail pages
    re.compile(r"(^|/)(?:[a-z]{2}(?:-[a-z]{2})?/)?sites?/jobsearch/job/\d{4,}(?:/|$|\?)", re.I),

    # Workday cxs/wday canonical detail URL
    re.compile(r"(^|/)wday/(?:jobs|cxs)/[^/]+/[^/]+/job/[^/]+_(?:JR|R|REQ)[-_]?\d{4,}(?:-\d+)?(?:/|$)", re.I),
]


def _looks_like_job_detail_url(url: str) -> bool:
    """
    Heuristic: does this absolute URL look like a job *detail* page?
    We err on the conservative side to avoid listing category pages.
    """
    parsed = urlparse(url)
    if not (parsed.scheme in ("http", "https") and parsed.netloc):
        return False

    path = parsed.path or "/"
    if not path.startswith("/"):
        path = "/" + path

    # Known slug/ID patterns
    if any(p.search(path) for p in JOB_DETAIL_PATTERNS):
        return True

    # ATS hosts commonly use /<org>/<uuid> or numeric ID as the leaf
    if _host_matches_ats(parsed.netloc):
        segs = [s for s in path.split("/") if s]
        if len(segs) >= 2 and (_UUID_RE.match(segs[-1]) or segs[-1].isdigit()):
            return True
        if any(s in {"job", "jobs", "openings"} for s in segs):
            return True

    # Narrow "job" fallback: ensure it's not a known non-detail section,
    # requires a reasonable slug after /job(s)/ and excludes obvious pagination.
    if "job" in path and "page=" not in url.lower():
        segs = [s for s in path.split("/") if s]
        if not (set(segs) & BAD_PATH_SEGMENTS):
            if re.search(r"/job[s]?/[\w-]{6,}(/|$)", path):
                return True

    return False


def _max_heading_text(node) -> str:
    heads = node.find_all(["h1", "h2", "h3", "h4", "h5", "h6"])
    texts = []
    for h in heads:
        txt = h.get_text(" ", strip=True)
        if txt:
            txt = re.sub(r"\s+", " ", txt)
            texts.append(txt)
    return max(texts, key=len) if texts else ""


def _title_from_aria(a) -> str:
    aria = (a.get("aria-label") or "").strip()
    m = re.match(r"(?i)(?:learn more about|view details for)\s+(.+)", aria)
    return m.group(1).strip() if m else ""


def _clean_anchor_text(a) -> str:
    txt = (a.get_text(" ", strip=True) or a.get("title") or "").strip()
    txt = re.sub(r"\s+", " ", txt)
    if re.fullmatch(r"(?i)(learn more|help|sign in|bookmark|share|apply)", txt):
        return ""
    return txt


def _title_from_attrs(node) -> str:
    # Check attributes that might already contain the title
    for attr in ["aria-label", "title"]:
        v = (node.get(attr) or "").strip()
        if v:
            m = re.match(r"(?i)(?:learn more about|view details for)\s+(.+)", v)
            return (m.group(1) if m else v).strip()
    return ""


_GENERIC_CLASS_TOKENS = {"row", "rows", "col", "cols", "container", "grid", "section", "wrapper", "content"}


def _is_generic_classkey(class_key: str) -> bool:
    toks = set(class_key.split())
    return bool(toks & _GENERIC_CLASS_TOKENS) or not class_key


def _selector_from_key(key: Tuple[str, str]) -> str:
    tag, class_key = key
    classes = [c for c in class_key.split() if c]
    return f"{tag}{''.join('.' + c for c in classes)}"
