from __future__ import annotations

import asyncio
import inspect
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable, Iterable, List, Optional, Tuple

from data.model import Job, JobBoard, load_pages
from services.scrape import scrape_jobs  # works whether sync or async


@dataclass(frozen=True)
class WorkerConfig:
    pages_dir: Path = Path("data/pages")
    # scrape cadence (base + jitter)
    base_frequency: timedelta = timedelta(hours=1)
    jitter: timedelta = timedelta(minutes=30)
    # never schedule earlier than this from "now"
    min_delay: timedelta = timedelta(minutes=5)
    # when a scrape fails, back off by:
    error_backoff: timedelta = timedelta(minutes=20)
    error_jitter: timedelta = timedelta(minutes=5)
    # how many pages to scrape at once
    concurrency: int = 3
    # optional: dry run (no file writes)
    dry_run: bool = False
    # optional: run a single pass and exit
    once: bool = False


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _rand_seconds(span: timedelta) -> float:
    return random.uniform(-span.total_seconds(), span.total_seconds())


def compute_next_scrape_at(
    *,
    last_scraped: Optional[datetime],
    now: Optional[datetime] = None,
    base: timedelta,
    jitter: timedelta,
    min_delay: timedelta,
) -> datetime:
    """
    next = (last_scraped or now) + base + U[-jitter,+jitter], but never earlier than now + min_delay
    """
    t0 = last_scraped or (now or _now_utc())
    current = now or _now_utc()
    candidate = t0 + base + timedelta(seconds=_rand_seconds(jitter))

    floor = current + min_delay
    if candidate < floor:
        # keep a little randomness so workers don't align perfectly
        candidate = floor + timedelta(seconds=random.uniform(0, 30))
    return candidate


async def _maybe_async_call(fn: Callable[..., List[Job] | Awaitable[List[Job]]], *args, **kwargs) -> List[Job]:
    """
    Call scrape_jobs whether it's sync or async.
    """
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result  # type: ignore[return-value]
    return result  # type: ignore[return-value]


async def _scrape_one(
    jf: Path,
    jb: JobBoard,
    cfg: WorkerConfig,
    *,
    now: Optional[datetime] = None,
) -> Tuple[Path, JobBoard, Optional[Exception], int]:
    """
    Scrape a single page file; returns (path, updated_jobboard, error, num_scraped)
    """
    now = now or _now_utc()
    num = 0
    err: Optional[Exception] = None
    try:
        jobs: List[Job] = await _maybe_async_call(scrape_jobs, str(jb.website_url))
        num = len(jobs)
        jb.apply_scrape(jobs, scraped_at=now)
        jb.next_scrape_at = compute_next_scrape_at(
            last_scraped=jb.last_scraped, now=now,
            base=cfg.base_frequency, jitter=cfg.jitter, min_delay=cfg.min_delay
        )
    except Exception as e:
        err = e
        # schedule a backoff retry
        base = cfg.error_backoff
        jitter = cfg.error_jitter
        jb.next_scrape_at = compute_next_scrape_at(
            last_scraped=now, now=now,
            base=base, jitter=jitter, min_delay=cfg.min_delay
        )
    return jf, jb, err, num


def _due_pages(items: Iterable[Tuple[Path, JobBoard]], now: datetime) -> list[Tuple[Path, JobBoard]]:
    due: list[Tuple[Path, JobBoard]] = []
    for jf, jb in items:
        nsa = jb.next_scrape_at
        if nsa is None or nsa <= now:
            due.append((jf, jb))
    return due


async def run_pages_worker(cfg: WorkerConfig) -> None:
    """
    Main loop. Loads pages, scrapes due ones with concurrency, writes results,
    updates next_scrape_at, repeats.
    """
    # pre-load to validate the folder exists even if empty
    cfg.pages_dir.mkdir(parents=True, exist_ok=True)

    sem = asyncio.Semaphore(cfg.concurrency)

    async def _task(jf: Path, jb: JobBoard, now: datetime):
        async with sem:
            path, updated, err, n = await _scrape_one(jf, jb, cfg, now=now)
            if err:
                print(f"[{now.isoformat()}] ERROR scraping {updated.title}: {err!r}")
            else:
                print(f"[{now.isoformat()}] scraped {updated.title}: {n} jobs")

            if not cfg.dry_run:
                # persist
                updated.to_file(path)
            else:
                print(f"[dry-run] would write {path}")

    while True:
        now = _now_utc()
        pages: list[Tuple[Path, JobBoard]] = load_pages(cfg.pages_dir)
        if not pages:
            # nothing to do — nap a bit
            await asyncio.sleep(1.0)
            if cfg.once:
                return
            continue

        # ensure first-time pages run now (with tiny jitter), not in +1h
        for _, jb in pages:
            if jb.next_scrape_at is None and jb.last_scraped is None:
                jb.next_scrape_at = compute_next_scrape_at(
                    last_scraped=None,
                    now=now,
                    base=timedelta(0),  # <-- first run: no base delay
                    jitter=timedelta(0),
                    min_delay=timedelta(0),  # allow "now"
                )

        due = _due_pages(pages, now)
        if due:
            tasks = [asyncio.create_task(_task(jf, jb, now)) for jf, jb in due[: cfg.concurrency * 2]]
            await asyncio.gather(*tasks)

        if cfg.once:
            return
        await asyncio.sleep(1.0)