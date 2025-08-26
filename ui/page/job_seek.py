import altair as alt
import pandas as pd
import streamlit as st
from datetime import datetime, timezone
from typing import Iterable
from data.model import load_pages       # your helper
from pathlib import Path


PAGES_DIR = Path("data/pages")  # adjust if needed


def _to_utc_pdts(dt) -> pd.Timestamp:
    if isinstance(dt, pd.Timestamp):
        if dt.tzinfo is None:
            return dt.tz_localize("UTC")
        return dt.tz_convert("UTC")
    if dt.tzinfo is None:
        return pd.Timestamp(dt, tz="UTC")
    return pd.Timestamp(dt.astimezone(timezone.utc))


def _job_active_intervals(job, *, upper: pd.Timestamp | None = None) -> Iterable[tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Yield [start, end) intervals in UTC where the job is active.
    Assumes history is already normalized (no dup-consecutive statuses).
    If `upper` is provided, cap open-ended intervals at this timestamp; otherwise use current time.
    """
    if upper is None:
        upper = pd.Timestamp(datetime.now(timezone.utc))
    else:
        upper = _to_utc_pdts(upper)

    start = None
    for ev in sorted(job.history, key=lambda s: s.at):
        t = _to_utc_pdts(ev.at)
        if ev.status == "active":
            if start is None:
                start = t
        else:  # treat any non-"active" as inactive boundary
            if start is not None and t > start:
                yield (start, t)
                start = None
    if start is not None:
        # If the last event is not "inactive", assume active up to the latest timestep (upper).
        yield (start, upper)


@st.fragment(run_every=30)
def dashboard():
    job_boards = [page for _, page in load_pages(PAGES_DIR)]
    if not job_boards:
        st.info("No pages loaded yet.")
        return

    # Discover overall time span (min event → latest event), all in UTC
    min_ts = None
    max_ts = None
    for board in job_boards:
        for job in board.content:
            for ev in job.history:
                t = _to_utc_pdts(ev.at)
                min_ts = t if min_ts is None or t < min_ts else min_ts
                max_ts = t if max_ts is None or t > max_ts else max_ts

    if min_ts is None or max_ts is None:
        st.info("No history found yet.")
        return

    # === Robust binning: edges vs. bins ===
    latest_edge = max_ts.ceil("6h")  # rightmost EDGE (not a bin start)
    edges = pd.date_range(start=min_ts.floor("6h"), end=latest_edge, freq="6h", tz="UTC")
    if len(edges) < 2:
        edges = pd.date_range(start=min_ts.floor("6h"), periods=2, freq="6h", tz="UTC")

    bin_starts = edges[:-1]  # rows correspond to [bin_start, next_edge)
    job_board_names = sorted({board.title for board in job_boards})
    df = pd.DataFrame(0, index=bin_starts, columns=job_board_names, dtype="int32")

    # Fast binning with searchsorted against EDGES; write into rows aligned to BIN STARTS.
    for board in job_boards:
        col = board.title
        if col not in df.columns:
            continue
        for job in board.content:
            for a, b in _job_active_intervals(job, upper=edges[-1]):
                # Clip interval to plotting window
                a = max(a, edges[0])
                b = min(b, edges[-1])  # open end at the rightmost edge
                if b <= a:
                    continue
                start_pos = int(edges.searchsorted(a, side="left"))
                end_pos   = int(edges.searchsorted(b, side="left"))
                if end_pos > start_pos:
                    df.iloc[start_pos:end_pos, df.columns.get_loc(col)] += 1

    if df.to_numpy().sum() == 0:
        st.info("No active postings in the selected period.")
        return

    # Melt and rename to "Job board"
    melted = (
        df.reset_index()
          .rename(columns={"index": "time"})
          .melt(id_vars="time", var_name="Job board", value_name="count")
    )

    chart = (
        alt.Chart(melted)
        .mark_area()
        .encode(
            x=alt.X("time:T", title="Time (UTC)"),
            y=alt.Y("count:Q", stack="zero", title="Active job postings"),
            color=alt.Color("Job board:N", title="Job board"),
            tooltip=[alt.Tooltip("time:T"), "Job board:N", "count:Q"],
        )
        .properties(height=340)
        .interactive(bind_y=False)  # zoom X only
    )

    with st.container(border=True, key="dashboard-holder"):
        st.altair_chart(chart, use_container_width=True, key="job-board-chart")


@st.fragment(run_every=30)
def new_jobs_list():
    pass


if __name__ == "__main__":
    st.set_page_config(page_title="Job Seek", layout="wide")
    dashboard()

    new_jobs_list()
