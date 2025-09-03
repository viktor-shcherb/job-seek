from operator import itemgetter

import altair as alt
import pandas as pd
import streamlit as st
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Tuple
from data.model import load_pages, JobBoard, Job  # your helper
from pathlib import Path

from services.image.logo_preprocess import preprocess_logo
from ui.cards.job import display_job

PAGES_DIR = Path("data/pages")  # adjust if needed


@st.fragment(run_every=30)
def dashboard():
    pages: list[tuple[Path, object]] = load_pages(PAGES_DIR)
    if not pages:
        st.info("No active postings in the selected period.")
        return

    # Consistent UTC handling (works for naive or tz-aware datetimes)
    def ensure_utc(dt) -> pd.Timestamp:
        ts = pd.Timestamp(dt)
        return ts.tz_localize("UTC") if ts.tz is None else ts.tz_convert("UTC")

    now_utc = pd.Timestamp.now("UTC")

    # Build active intervals per job from status history
    def active_intervals(job) -> list[tuple[datetime, datetime]]:
        hist = sorted(job.history, key=lambda s: s.at)
        intervals: list[tuple[datetime, datetime]] = []
        cur_start = None
        for ev in hist:
            if ev.status == "active":
                if cur_start is None:
                    cur_start = ev.at
            else:  # inactive
                if cur_start is not None:
                    if ev.at > cur_start:
                        intervals.append((cur_start, ev.at))
                    cur_start = None
        if cur_start is not None:
            intervals.append((cur_start, now_utc))
        return intervals

    # Helper: active/new as-of a given time
    def is_active_at(job, t: pd.Timestamp) -> bool:
        t = ensure_utc(t)
        for s, e in active_intervals(job):
            if ensure_utc(s) <= t < ensure_utc(e):
                return True
        return False

    def first_active_at(job) -> pd.Timestamp | None:
        for ev in sorted(job.history, key=lambda s: s.at):
            if ev.status == "active":
                return ensure_utc(ev.at)
        return None

    # Collect intervals per board and determine global time span
    board_intervals: dict[str, list[tuple[datetime, datetime]]] = {}
    all_starts, all_ends = [], []
    all_jobs = []

    for _, board in pages:
        intervals: list[tuple[datetime, datetime]] = []
        for job in board.content:
            all_jobs.append(job)
            for s, e in active_intervals(job):
                intervals.append((s, e))
                all_starts.append(s)
                all_ends.append(e)
        board_intervals[board.title] = intervals

    if not all_starts or not all_ends:
        st.info("No active postings in the selected period.")
        return

    start_ts = ensure_utc(min(all_starts))
    end_ts = ensure_utc(max(max(all_ends), now_utc))
    freq = "30min"

    # Note: start/end are tz-aware UTC; don't pass tz=...
    times = pd.date_range(start=start_ts, end=end_ts, freq=freq)
    if len(times) == 0:
        st.info("No active postings in the selected period.")
        return

    df = pd.DataFrame(index=times)

    # Build step count series per board using ONLY +1/-1 deltas (no baseline seeding)
    for board_title, intervals in board_intervals.items():
        if not intervals:
            df[board_title] = 0
            continue

        events: dict[pd.Timestamp, int] = {}
        for s, e in intervals:
            s_ts = ensure_utc(s)
            e_ts = ensure_utc(e)
            if e_ts <= s_ts:
                continue
            events[s_ts] = events.get(s_ts, 0) + 1
            events[e_ts] = events.get(e_ts, 0) - 1

        if not events:
            df[board_title] = 0
            continue

        ev_series = pd.Series(events).sort_index().cumsum()
        counts = ev_series.reindex(times, method="ffill").fillna(0).astype(int)
        df[board_title] = counts.values

    # All zeros?
    if df.to_numpy().sum() == 0:
        st.info("No active postings in the selected period.")
        return

    # Melt and rename to "Job board"
    melted = (
        df.reset_index()
          .rename(columns={"index": "time"})
          .melt(id_vars="time", var_name="Job board", value_name="count")
    )

    # --- helper to build a legend chart for a subset of categories ---
    def make_legend_chart(names, n_cols, color_scale, step_x=140, step_y=28):
        df = pd.DataFrame({"Job board": names})
        df["idx"] = range(len(df))
        df["col"] = df["idx"] % n_cols
        df["row"] = df["idx"] // n_cols

        x_pos = alt.X("col:O", axis=None, scale=alt.Scale(padding=0))
        y_pos = alt.Y("row:O", axis=None, scale=alt.Scale(padding=0))

        pts = (
            alt.Chart(df)
            .mark_square(size=110)
            .encode(
                x=x_pos,
                y=y_pos,
                color=alt.Color("Job board:N", scale=color_scale, legend=None),
                tooltip=["Job board:N"],
            )
        )
        lbl = (
            alt.Chart(df)
            .mark_text(align="left", dx=8, dy=1)
            .encode(
                x=x_pos,
                y=y_pos,
                text="Job board:N",
                color=alt.Color("Job board:N", scale=color_scale, legend=None),
            )
        )
        return (pts + lbl).properties(
            width=alt.Step(step_x),
            height=alt.Step(step_y),
            padding=0
        ).configure_view(stroke=None)

    # --- build two legend panels with the same color scale/domain ---
    cats = sorted(melted["Job board"].unique().tolist())
    color_scale = alt.Scale(domain=cats, scheme="tableau20")

    half = (len(cats) + 1) // 2  # left gets the extra one if odd
    left_cats = cats[:half]
    right_cats = cats[half:]

    legend_left = make_legend_chart(left_cats, n_cols=2, color_scale=color_scale)
    legend_right = make_legend_chart(right_cats, n_cols=2, color_scale=color_scale)

    # --- main chart (unchanged) ---
    chart = (
        alt.Chart(melted)
        .mark_area()
        .encode(
            x=alt.X("time:T", title="Time (UTC)"),
            y=alt.Y("count:Q", stack="zero", title="Active job postings"),
            color=alt.Color("Job board:N", scale=color_scale, legend=None),
            tooltip=[alt.Tooltip("time:T"), "Job board:N", "count:Q"],
        )
        .properties(height=500)
        .interactive(bind_y=False)
    )

    with st.container(border=True, key="dashboard-holder"):
        st.altair_chart(chart, use_container_width=True, key="job-board-chart")

        # responsive-friendly: two legends in a horizontal container
        with st.container(horizontal=True, key="legend-row", gap="large"):
            st.altair_chart(legend_left, use_container_width=True, key="legend-left")
            st.altair_chart(legend_right, use_container_width=True, key="legend-right")

        # ── Metrics: current counts and Δ vs 24h ago ─────────────────────────────
        t_24 = now_utc - pd.Timedelta(hours=24)
        threshold = timedelta(hours=48)

        # Active now / 24h ago
        active_now = sum(1 for j in all_jobs if j.is_active())
        active_24h_ago = sum(1 for j in all_jobs if is_active_at(j, t_24))
        active_delta = active_now - active_24h_ago

        # New (≤24h) now / 24h ago (and active at that time)
        def is_new_as_of(j, t: pd.Timestamp, thr: timedelta = threshold) -> bool:
            fa = first_active_at(j)
            return (
                fa is not None
                and t >= fa
                and (t - fa) <= thr
                and is_active_at(j, t)
            )

        new_now = sum(1 for j in all_jobs if j.is_active() and j.is_new())  # uses Job.is_new() default (24h)
        new_24h_ago = sum(1 for j in all_jobs if is_new_as_of(j, t_24))
        new_delta = new_now - new_24h_ago

        with st.container(horizontal=True, horizontal_alignment="left", key="metrics-container", gap="medium"):
            st.metric("Active jobs", active_now, delta=active_delta, help="Change vs 24h ago (UTC).", width="content")
            st.metric("New (≤48h)", new_now, delta=new_delta, help="Change vs 24h ago (UTC).", width="content")


@st.fragment(run_every=30)
def new_jobs_list():
    all_jobs: List[Tuple[JobBoard, Job]] = []
    for path, job_board in load_pages(PAGES_DIR):
        for idx, job in enumerate(job_board.content):
            if job.is_new() and job.is_active():
                all_jobs.append((job_board, job))

    all_jobs = sorted(all_jobs, key=lambda j: j[1].age())

    with st.container(border=True, key="new-jobs-holder"):
        for job_idx, (job_board, job) in enumerate(all_jobs):
            display_job(f"job-{job_idx}", job, include_logo=job_board.icon_url)


if __name__ == "__main__":
    st.set_page_config(page_title="Job Seek", layout="wide")
    dashboard()
    new_jobs_list()
