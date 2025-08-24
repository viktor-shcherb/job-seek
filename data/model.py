from __future__ import annotations
import json
import re
import unicodedata
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Tuple, Literal

from cachetools.func import ttl_cache
from pydantic import BaseModel, AnyUrl, Field, ValidationError, model_validator


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


class Status(BaseModel):
    status: Literal["active", "inactive"]
    # Timestamp (UTC) of when this status was observed/changed
    at: datetime = Field(default_factory=now_utc)


_FLAP_WINDOW = timedelta(hours=6)


def _normalize_history(history: list[Status]) -> list[Status]:
    """
    Normalize history:
      - Sort by timestamp ascending.
      - Collapse consecutive duplicate statuses (keep earliest in each run).
      - If we see an 'active' → 'inactive' → 'active' within _FLAP_WINDOW,
        treat it as never inactive: keep the first 'active', drop the
        'inactive' and the returning 'active'.
    """
    if not history:
        return []

    hist = sorted(history, key=lambda s: s.at)
    out: list[Status] = []

    for st in hist:
        if out:
            # 1) Drop consecutive duplicates
            if out[-1].status == st.status:
                continue

            # 2) Collapse short active→inactive→active flaps
            if len(out) >= 2:
                a1, a2 = out[-2], out[-1]
                if a1.status == "active" and a2.status == "inactive" and st.status == "active":
                    # If we bounced back to active quickly, remove the flap.
                    if st.at - a1.at <= _FLAP_WINDOW:
                        # Remove the middle 'inactive' and skip appending current 'active'
                        out.pop()  # remove the 'inactive'
                        continue

        out.append(st)

    return out


class Job(BaseModel):
    # Uniquely identified by (title, link). In practice, the link should be unique.
    title: str = Field(..., min_length=1)
    link: AnyUrl
    history: list[Status] = Field(default_factory=list)

    @model_validator(mode="after")
    def _normalize(self) -> "Job":
        self.history = _normalize_history(self.history)
        return self

    def is_active(self) -> bool:
        return bool(self.history) and self.history[-1].status == "active"

    def active_hours(self) -> float:
        # If not currently active, latest active streak length is 0
        if not self.is_active():
            return 0.0

        # Walk backwards until the most recent 'inactive' (the streak boundary)
        start: Optional[datetime] = None
        for st in reversed(self.history):
            if st.status == "inactive":
                break
            start = st.at  # earliest 'active' in the trailing active block

        if start is None:
            return 0.0  # defensive

        # Compute hours from the streak start until now (UTC)
        delta = now_utc() - start
        return max(0.0, delta.total_seconds() / 3600.0)

    def mark(self, new_status: Literal["active", "inactive"], at: Optional[datetime] = None) -> None:
        """
        Append a status-change event unless it would duplicate the last event.
        """
        ts = at or now_utc()
        if self.history and self.history[-1].status == new_status:
            # Rule: do not add event if last event has the same status
            return
        self.history.append(Status(status=new_status, at=ts))
        # Keep tidy immediately
        self.history = _normalize_history(self.history)


class JobBoard(BaseModel):
    title: str = Field(..., min_length=1)
    icon_url: AnyUrl
    website_url: AnyUrl
    last_scraped: Optional[datetime] = None
    next_scrape_at: Optional[datetime] = None
    content: list[Job] = Field(default_factory=list)

    # --- File IO ---
    @classmethod
    @ttl_cache(ttl=30)
    def from_file(cls, path: Path) -> "JobBoard":
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(**data)

    def to_file(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        text = self.model_dump_json(indent=2)
        path.write_text(text, encoding="utf-8")

    # --- Helpers for maintaining history ---
    def apply_scrape(self, scraped_jobs: list[Job], scraped_at: Optional[datetime] = None) -> None:
        """
        Merge 'currently active' scraped jobs into this page:
          - Jobs present in scrape -> ensure final status is 'active'
          - Jobs missing from scrape but were active -> add 'inactive'
          - Preserve & normalize history
        """
        ts = scraped_at or now_utc()

        # Index current jobs by link (primary key)
        by_link: dict[str, Job] = {str(j.link): j for j in self.content}
        scraped_by_link: dict[str, Job] = {str(j.link): j for j in scraped_jobs}

        # Activate / upsert scraped jobs
        for link, new_job in scraped_by_link.items():
            if link in by_link:
                cur = by_link[link]
                # Update title if it changed
                if new_job.title and new_job.title != cur.title:
                    cur.title = new_job.title
                cur.mark("active", ts)  # no-op if already active
            else:
                # New job, mark as active now
                new_job.history = _normalize_history([Status(status="active", at=ts)])
                self.content.append(new_job)

        # Deactivate jobs missing from the scrape
        for link, existing in by_link.items():
            if link not in scraped_by_link and existing.is_active():
                existing.mark("inactive", ts)  # no-op if already inactive

        # Update timestamp
        self.last_scraped = ts

        # Optional: sort active first, then title
        self.content.sort(key=lambda j: (0 if j.is_active() else 1, j.title.lower()))


def slugify(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
    return value or "page"


def list_page_files(pages_dir: Path) -> list[Path]:
    pages_dir.mkdir(parents=True, exist_ok=True)
    return sorted(p for p in pages_dir.glob("*.json") if p.is_file())


def load_pages(pages_dir: Path) -> list[Tuple[Path, JobBoard]]:
    out: list[Tuple[Path, JobBoard]] = []
    for jf in list_page_files(pages_dir):
        try:
            out.append((jf, JobBoard.from_file(jf)))
        except ValidationError:
            # Skip invalid JSONs; you could log these if you want.
            continue
    return out
