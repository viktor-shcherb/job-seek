from bs4 import BeautifulSoup

# Optional: extract these to module scope if re-used elsewhere.
_MOUNT_SELECTORS = [
    "#__next", "#root", "#app", "[data-reactroot]", "[ng-app]",
    ".search-results-app", "[data-buycard-app]"
]
_JS_HINT_STRINGS = [
    "enable javascript", "turn on javascript", "requires javascript",
    "needs javascript", "please enable cookies", "disabled scripts"
]

def looks_js_shell(html: str) -> bool:
    if not html:
        return True

    soup = BeautifulSoup(html, "html.parser")

    # Original signal
    real_nodes = len(soup.find_all(lambda t: t.name not in {"script", "style"}))
    scripts = len(soup.find_all("script"))
    body_text = (soup.body.get_text(" ", strip=True) if soup.body else "").lower()
    hints = any(h in body_text for h in _JS_HINT_STRINGS)

    # New: common SPA mount points present?
    has_mount = any(soup.select_one(sel) for sel in _MOUNT_SELECTORS)

    # New: “heavy DOM, low text” — many elements but very little meaningful text in main content.
    main = soup.main or soup.body
    text_len = len(main.get_text(" ", strip=True)) if main else 0
    low_content = (real_nodes > 200 and text_len < 800 and scripts >= 3)

    # New: Esri careers job-search shell is an empty app container that JS fills.
    esri_shell = bool(soup.select_one(".sra.search-results-app")) or bool(
        soup.select_one('[data-buycard-app="careers"]')
    )

    spinner = bool(soup.select_one(".app-loading-spinner"))

    # Keep the original small-DOM rule, add new triggers.
    return ((real_nodes < 15 and scripts >= 3) or hints or has_mount or low_content or esri_shell or spinner)
