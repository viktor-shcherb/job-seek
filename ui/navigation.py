from __future__ import annotations
import sys
from pathlib import Path
import streamlit as st

from data.model import load_pages, slugify, JobBoard
from ui.page_renderer import run_page  # imported so the generated stubs can import it


ROOT = Path(__file__).resolve().parents[1]
PAGES_DIR = ROOT / "data" / "pages"
GEN_PAGES_DIR = ROOT / "page" / "generated"
ADD_PAGE_SCRIPT = ROOT / "page" / "add_job_board.py"


def _ensure_generated_page_scripts():
    """
    For each JSON page, create a tiny script in page/generated/
    that calls ui.page_renderer.run_page(<slug>).
    """
    GEN_PAGES_DIR.mkdir(parents=True, exist_ok=True)

    for i, (_, page) in enumerate(load_pages(PAGES_DIR), start=1):
        slug = slugify(page.title)
        script_path = GEN_PAGES_DIR / f"{slug}.py"
        content = (
            "from ui.page_renderer import run_page\n"
            f"run_page({slug!r})\n"
        )
        # Write only if missing or changed
        if not script_path.exists() or script_path.read_text(encoding="utf-8") != content:
            script_path.write_text(content, encoding="utf-8")


def get_active_pages() -> list[st.Page]:
    _ensure_generated_page_scripts()

    # First page (non-data): Add new page form
    pages: list[st.Page] = [
        st.Page(
            str(ADD_PAGE_SCRIPT),
            title="Add new job board",
            icon=":material/add_circle:",
            url_path="add"
        )
    ]

    # Then one page per JSON config
    for script in sorted(GEN_PAGES_DIR.glob("*.py")):
        slug = script.stem
        page = JobBoard.from_file(PAGES_DIR / f"{slug}.json")
        pages.append(
            st.Page(
                str(script),
                title=page.title,
                icon=":material/work:",
                url_path=slug
            )
        )
    # Make the Add page the default landing page
    if pages:
        pages[0].default = True  # type: ignore[attr-defined]
    return pages


def setup_navigation():
    """
    Build and return the navigation object. Caller should do nav.run().
    """
    # Make root importable when running `streamlit run app.py`
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    pages = get_active_pages()
    # Show sidebar and expand it; you can tweak this logic if you have auth, etc.
    nav = st.navigation(pages, position="sidebar", expanded=True)
    return nav
