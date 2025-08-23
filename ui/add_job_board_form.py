from __future__ import annotations
from pathlib import Path
import streamlit as st
from data.model import JobBoard, slugify, load_pages  # <- import load_pages
from services.image.logo_preprocess import preprocess_logo

PAGES_DIR = Path(__file__).resolve().parents[1] / "data" / "pages"
PAGES_SOURCE_DIR = Path(__file__).resolve().parents[1] / "page" / "generated"


def _delete_page(slug: str):
    json_file = PAGES_DIR / f"{slug}.json"
    json_file.unlink()
    py_file = PAGES_SOURCE_DIR / f"{slug}.py"
    py_file.unlink()


@st.fragment()
def render_add_page_form():
    st.set_page_config(page_title="Job Seek")

    with st.form("add_page_form", clear_on_submit=True):
        title_col, icon_col = st.columns(2)
        with title_col:
            title = st.text_input("Title", placeholder="e.g., Google")
        with icon_col:
            icon_url = st.text_input("Icon URL", placeholder="https://.../favicon.ico")

        website_url = st.text_input("Job Board URL", placeholder="https://www.example.com")
        # If these container kwargs don't exist in your Streamlit version, remove them.
        with st.container(horizontal=True, horizontal_alignment="right", key="submit-container"):
            submitted = st.form_submit_button("Add Job Board", type="primary")

    status_holder = st.empty()

    if submitted:
        if not title or not website_url or not icon_url:
            status_holder.error("Please fill Title, Website URL, and Icon URL.")
            return

        page = JobBoard(
            title=title.strip(),
            website_url=website_url.strip(),
            icon_url=icon_url.strip(),
            content=[],
        )

        filename = f"{slugify(page.title)}.json"
        dest = PAGES_DIR / filename
        if dest.exists():
            status_holder.warning("A job board with this title already exists.")
        else:
            try:
                page.to_file(dest)
                status_holder.success(f"JobBoard created: {page.title}")
                st.rerun()  # triggers a rebuild of navigation
            except Exception as e:
                status_holder.error(f"Could not save file: {e}")

    # -------- Current job boards list --------
    st.subheader("Active job boards")
    pages = load_pages(PAGES_DIR)

    if not pages:
        st.info("No job boards yet.")
        return

    with st.container(key="board-list-container"):
        for file_path, page in pages:
            with st.container(
                border=True,
                horizontal=True,
                horizontal_alignment="left",
                vertical_alignment="center",
                key=f"board-{file_path.stem}"
            ):
                with st.container(
                    horizontal=True,
                    horizontal_alignment="left",
                    vertical_alignment="center",
                    key=f"board-icon-title-{file_path.stem}"
                ):
                    st.image(preprocess_logo(str(page.icon_url)), width=64)
                    st.markdown(f"**{page.title}**")

                with st.container(
                    horizontal=True,
                    horizontal_alignment="right",
                    vertical_alignment="top",
                    key=f"controls-{file_path.stem}"
                ):
                    st.link_button("To Job Board", str(page.website_url))
                    if st.button(
                            ":material/delete:",
                            key=f"delete-{file_path.stem}",
                            help="Delete",
                            type="primary",
                        ):
                        _delete_page(file_path.stem)
                        st.rerun(scope="app")
