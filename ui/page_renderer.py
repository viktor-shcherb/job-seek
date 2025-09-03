from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import streamlit as st
from data.model import load_pages, slugify, JobBoard
from services.image.logo_preprocess import preprocess_logo
from ui.cards.job import display_job

PAGES_DIR = Path(__file__).resolve().parents[1] / "data" / "pages"

def run_page(slug: str):
    st.set_page_config(page_title="Job Seek", layout="centered")

    # Find the page by slug (derived from its title)
    for _, page in load_pages(PAGES_DIR):
        if slugify(page.title) == slug:
            _render_page(slug)
            break
    else:
        st.error(f"JobBoard not found for slug '{slug}'.")
        st.stop()


def _refresh(slug: str):
    path = PAGES_DIR / f"{slug}.json"
    jb = JobBoard.from_file(path)
    jb.last_scraped = None
    jb.next_scrape_at = None
    jb.to_file(path)


@st.fragment(run_every=5)
def _render_page(slug: str):
    path = PAGES_DIR / f"{slug}.json"
    page = JobBoard.from_file(path)

    # Header with icon + title
    col_logo, col_btn = st.columns([4, 1], vertical_alignment="bottom")
    with col_logo.container(horizontal_alignment="left", horizontal=True, vertical_alignment="bottom"):
        st.image(preprocess_logo(str(page.icon_url)), width=64)
        st.title(page.title)

    with col_btn.container(horizontal_alignment="right", horizontal=True, vertical_alignment="bottom"):
        st.link_button("To Job Board", str(page.website_url))

    if page.content:
        with st.container(
            key=f"refresh-{slug}",
            horizontal=True,
            horizontal_alignment="right",
            vertical_alignment="center"
        ):
            if page.last_scraped:
                st.info(
                    f"Last updated "
                    f"{(datetime.now(tz=page.last_scraped.tzinfo) - page.last_scraped).seconds / 60:.1f} "
                    f"minutes ago."
                )
            else:
                st.warning("Scraping is in progress...")

            st.button(
                ":material/refresh:",
                key=f"refresh-btn-{slug}",
                type="primary",
                help="Refresh the job listings",
                on_click=_refresh,
                args=(slug,)
            )

        with st.container(key=f"list-{slug}", border=True):
            filter_criteria = st.pills(
                "Filter jobs",
                ["active", "new"],
                selection_mode="multi",
                key="filter",
                default=["active"]
            )

            # Sort: active jobs first; among active, least age() first.
            sorted_jobs = sorted(
                page.content,
                key=lambda j: (not j.is_active(), j.age() if j.is_active() else float("inf"))
            )

            for job_idx, job in enumerate(sorted_jobs):
                if "new" in filter_criteria and not job.is_new():
                    continue
                if "active" in filter_criteria and not job.is_active():
                    continue

                job_id = f"job-{slug}-{job_idx}"
                display_job(job_id, job)
    else:
        st.info("No jobs yet.")
