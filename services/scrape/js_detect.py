# services/scrape/js_detect.py
from bs4 import BeautifulSoup


def looks_js_shell(html: str) -> bool:
    if not html:
        return True
    soup = BeautifulSoup(html, "html.parser")
    # Very few real nodes + lots of scripts, or "enable JavaScript" style hints
    real_nodes = len(soup.find_all(lambda t: t.name not in {"script", "style"}))
    scripts = len(soup.find_all("script"))
    body_text = (soup.body.get_text(" ", strip=True) if soup.body else "").lower()
    hints = ("enable javascript" in body_text) or ("turn on javascript" in body_text)
    return (real_nodes < 15 and scripts >= 3) or hints
