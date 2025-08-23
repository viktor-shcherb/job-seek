from __future__ import annotations
import sys
from pathlib import Path
import threading
import asyncio
import streamlit as st

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.navigation import setup_navigation  # noqa: E402
from services.worker import run_pages_worker, WorkerConfig  # noqa: E402


@st.cache_resource(show_spinner=False)
def start_pages_worker() -> threading.Thread:
    """Launch the background worker once per Streamlit server."""
    cfg = WorkerConfig()

    def _runner():
        # new event loop inside the thread
        asyncio.run(run_pages_worker(cfg))

    t = threading.Thread(target=_runner, name="pages-worker", daemon=True)
    t.start()
    return t

def main():
    # start the worker (non-blocking)
    start_pages_worker()

    # now render your app
    nav = setup_navigation()
    nav.run()

if __name__ == "__main__":
    main()
