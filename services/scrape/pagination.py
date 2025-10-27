# services/scrape/pagination.py
from __future__ import annotations

import re
from urllib.parse import urlparse, parse_qsl, urlunparse, urlencode

from services.scrape.url import _absolute

_NEXT_LABELS = ("next", "go to next page", "weiter", "suivant", "siguiente")

# Some sites use alternatives to `page`
_ALT_PAGE_KEYS = ("p", "pg", "pageNo", "pageNumber", "currentPage")


def _update_query_param(url: str, key: str, value: int | str) -> str:
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q[key] = str(value)
    return urlunparse(p._replace(query=urlencode(q, doseq=True)))


def _get_int_text(el) -> int | None:
    if not el:
        return None
    s = (el.get_text(strip=True) or "").replace(",", "")
    return int(s) if s.isdigit() else None


def _parse_results_window(soup) -> tuple[int | None, int | None]:
    """
    Parse '1-20 of 25' → (pagesize=20, total=25). Works for a few UIs.
    """
    text_nodes = soup.find_all(string=True)
    for t in text_nodes:
        s = " ".join(str(t).split())
        m = re.search(r"(\d+)\s*[\u2010\u2011\u2012\u2013\-]\s*(\d+)\s*of\s*(\d+)", s, flags=re.I)
        if m:
            start, end, total = map(int, m.groups())
            pagesize = end - start + 1 if end >= start else None
            return pagesize, total
    return None, None


def _current_page_from_dom(soup) -> int | None:
    # Apple: <input data-autom="paginationPageInput" value="1">
    inp = soup.select_one('input[data-autom="paginationPageInput"], input.rc-pagination-pageinput')
    if inp and inp.has_attr("value"):
        try:
            return int(inp["value"])
        except Exception:
            pass
    # Aria-live 'Page 1' style
    for el in soup.find_all(attrs={"aria-live": True}):
        m = re.search(r"page\s+(\d+)", (el.get_text(" ", strip=True) or ""), flags=re.I)
        if m:
            return int(m.group(1))
    return None


def _total_pages_from_dom(soup) -> int | None:
    # Apple: <span class="rc-pagination-total-pages">5</span>
    total = _get_int_text(soup.select_one(".rc-pagination-total-pages"))
    return total


def _find_next_href_direct(soup, current_page: int | None = None) -> str | None:
    """
    Try to find an explicit 'next' link in the DOM (anchor or button-wrapped anchor).
    """
    # 1) rel=next
    a = soup.select_one('a[rel*="next" i]')
    if a and a.has_attr("href"):
        return a["href"]

    # 2) anchors with aria-label mentioning "next" and not disabled
    for a in soup.select('a[aria-label]'):
        label = a.get("aria-label", "").lower()
        if re.search(r"\b(next|go to next page|weiter|suivant|siguiente)\b", label, flags=re.I):
            if a.get("aria-disabled", "").lower() in {"true", "1"}:
                continue
            if "disabled" in (a.get("class") or []):
                continue
            return a["href"]

    # 3) known button-wrapped anchor (Google-like)
    btn_next = soup.select_one(
        '[data-analytics-pagination="next"] a[href], '
        '.VfPpkd-wZVHld-gruSEe a[href][aria-label*="next" i]'
    )
    if btn_next:
        return btn_next.get("href")

    # 4) generic pager nav
    nav = soup.select_one('nav[aria-label*="pagination" i]')
    if nav:
        cand = nav.select_one('a[href][rel*="next" i], a[href][aria-label*="next" i]')
        if cand:
            return cand["href"]

    # 5) last-resort: look for anchors with ?page=K (or variants) > current
    keys = ("page",) + _ALT_PAGE_KEYS
    candidates: list[tuple[int, str]] = []
    for a in soup.select('nav[aria-label*="pagination" i] a[aria-label], ul.pagination a[aria-label], .pagination a[aria-label]'):
        href = a.get("href", "")
        for key in keys:
            m = re.search(rf"[?&]{re.escape(key)}=(\d+)\b", href)
            if m:
                k = int(m.group(1))
                if current_page is None or k > current_page:
                    candidates.append((k, href))
                break
    if candidates:
        candidates.sort()
        return candidates[0][1]

    return None


def discover_next_page_url(soup, base_url: str, current_url: str) -> str | None:
    """
    Return absolute URL for the next results page or None if we can't find or construct it.
    """
    # A) explicit link present?
    current_page = _current_page_from_dom(soup)
    href = _find_next_href_direct(soup, current_page=current_page)
    if href:
        try:
            return _absolute(href, base_url)
        except Exception:
            pass

    # B) Build from known paging params if we can infer current/total
    current_page = _current_page_from_dom(soup)
    total_pages = _total_pages_from_dom(soup)

    parsed = urlparse(current_url)
    qs = dict(parse_qsl(parsed.query, keep_blank_values=True))

    # If current has ?page=N → increment
    if "page" in qs:
        try:
            cur = int(qs.get("page", "1"))
            nxt = cur + 1
            if total_pages and nxt > total_pages:
                return None
            return _update_query_param(current_url, "page", nxt)
        except Exception:
            pass

    # Common alt page keys
    for key in _ALT_PAGE_KEYS:
        if key in qs:
            try:
                cur = int(qs.get(key, "1"))
                nxt = cur + 1
                if total_pages and nxt > total_pages:
                    return None
                return _update_query_param(current_url, key, nxt)
            except Exception:
                pass

    # Workday-like 'start' param (use results window to compute step)
    for k in ("start", "offset", "from", "startrow"):
        if k in qs:
            pagesize, total = _parse_results_window(soup)
            try:
                cur = int(qs[k])
            except Exception:
                cur = 0
            if pagesize:
                nxt = cur + pagesize
                if total and nxt >= total:
                    return None
                return _update_query_param(current_url, k, nxt)

    # If DOM exposes current/total and we find any anchor with ?page=k, use its key/pattern
    if current_page and (not total_pages or current_page < total_pages):
        for a in soup.find_all("a", href=True):
            m = re.search(r"[?&](\w+)=\d+\b", a["href"])
            if m:
                key = m.group(1)
                return _update_query_param(current_url, key, current_page + 1)

    return None
