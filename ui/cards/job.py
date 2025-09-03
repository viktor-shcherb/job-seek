import streamlit as st

from datetime import timedelta

from data.model import Job
from services.image.logo_preprocess import preprocess_logo


def display_job(slug: str, job: Job, *, include_logo: str | None = None):
    badges = []

    # Age badge (always shown)
    age_td = job.age()
    age_hours = age_td.total_seconds() / 3600.0
    if age_hours < 48:
        age_str = f"{age_hours:.1f}h"
    else:
        age_days = age_hours / 24.0
        age_str = f"{age_days:.1f}d"
    badges.append(f":violet-badge[{age_str}]")

    if job.is_active():
        badges.append(":red-badge[active]")
        badge_help = "Included in the latest scrape."

        # 'new' by default is <48h
        if job.is_new(threshold=timedelta(hours=48)):
            badges.append(":blue-badge[new]")
            badge_help = f"First scraped {age_str} ago."
        elif age_hours >= 24 * 7:
            badges.append(":gray-badge[old]")
            badge_help = f"First scraped {age_hours / 24.0:.1f} days ago."
    else:
        badges.append(":gray-badge[inactive]")
        badge_help = "Not included in the latest scrape."

    with st.container(
            key=f"container-{slug}",
            border=True,
            horizontal=True,
            horizontal_alignment="distribute",
    ):
        with st.container(key=f"logo-desc-{slug}", horizontal=True):
            if include_logo:
                st.image(preprocess_logo(include_logo), width=64)
            with st.container(key=f"desc-{slug}"):
                st.markdown(f"**{job.title}**")
                st.markdown(" ".join(badges), help=badge_help)
        st.link_button("Apply", str(job.link), type="primary")